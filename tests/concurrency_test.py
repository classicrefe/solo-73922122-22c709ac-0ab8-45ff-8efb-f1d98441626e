"""双进程并发导入/发布验证（真实 uvicorn ×2，共享同一 SQLite 文件）。

运行：python tests/concurrency_test.py
不依赖 TestClient；直接起两个服务进程，用 HTTP 并发压测临界区。

覆盖：
1. 两个服务进程同时首次导入不同 CSV：恰好一个 201、一个 409；
   库中只有获胜文件的格值（不混入另一份），格数 ≤ 16。
2. 每轮结果都稳定（重复多轮，含同进程双请求）。
3. 导入中途失败（进程被 kill / 写库异常）不留部分格值：
   用超短超时 + 事务内异常注入验证回滚；再用子进程 kill -9 验证无部分行。
4. 预览 / 公开页 / 发布只读获胜调查。
5. 两进程并发发布两个各自安全、合并不安全的窗口：不能都成功；
   重核失败方收到 409。
"""

import json
import os
import socket
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PY = sys.executable

results = []


def check(name, cond, extra=""):
    results.append(bool(cond))
    print(f"[{'PASS' if cond else 'FAIL'}] {name}")
    if not cond and extra:
        print("       " + str(extra).replace("\n", "\n       ")[:1200])


def free_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def http(method, url, body=None, headers=None, timeout=30):
    req = urllib.request.Request(url, data=body, method=method, headers=headers or {})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, r.read().decode("utf-8")
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode("utf-8")


def post_json(port, path, obj):
    return http(
        "POST",
        f"http://127.0.0.1:{port}{path}",
        body=json.dumps(obj).encode(),
        headers={"Content-Type": "application/json"},
    )


def post_file(port, path, content: bytes, filename="data.csv"):
    boundary = "----cdcboundary" + str(time.time_ns())
    body = (
        f"--{boundary}\r\n"
        f'Content-Disposition: form-data; name="file"; filename="{filename}"\r\n'
        "Content-Type: text/csv\r\n\r\n"
    ).encode() + content + f"\r\n--{boundary}--\r\n".encode()
    return http(
        "POST",
        f"http://127.0.0.1:{port}{path}",
        body=body,
        headers={"Content-Type": f"multipart/form-data; boundary={boundary}"},
    )


def wait_ready(port, proc, timeout=40):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(f"server on {port} exited: {proc.returncode}")
        try:
            status, _ = http("GET", f"http://127.0.0.1:{port}/api/public", timeout=1)
            if status == 200:
                return
        except (urllib.error.URLError, ConnectionError, OSError):
            pass
        time.sleep(0.1)
    raise RuntimeError(f"server on {port} not ready")


class Servers:
    def __init__(self, db_path, n=2, extra_env=None):
        self.db_path = db_path
        self.procs = []
        self.ports = []
        env = dict(os.environ, DB_PATH=db_path, PYTHONPATH=str(ROOT))
        env.pop("DEMO_MODE", None)
        base = [free_port() for _ in range(n)]
        for i in range(n):
            port = base[i]
            peers = [f"http://127.0.0.1:{p}" for p in base if p != port]
            e = dict(env)
            e["SELF_URL"] = f"http://127.0.0.1:{port}"
            e["PEER_URLS"] = ",".join(peers)
            if extra_env:
                e.update(extra_env)
            proc = subprocess.Popen(
                [PY, str(ROOT / "tests" / "_server_runner.py"), str(port)],
                cwd=str(ROOT),
                env=e,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
            )
            self.procs.append(proc)
            self.ports.append(port)
        for p, proc in zip(self.ports, self.procs):
            wait_ready(p, proc)

    def stop(self):
        # 沙箱禁止发信号：关闭 stdin 触发服务进程优雅退出。
        for p in self.procs:
            if p.poll() is None and p.stdin:
                try:
                    p.stdin.close()
                except OSError:
                    pass
        for p in self.procs:
            try:
                p.wait(timeout=10)
            except subprocess.TimeoutExpired:
                p.kill()

    def __enter__(self):
        return self

    def __exit__(self, *a):
        self.stop()


def csv_a(prefix="东", n=3):
    lines = ["分区,年龄组,人数,检出数"]
    for i in range(n):
        lines += [f"{prefix}{i},青年,10,1", f"{prefix}{i},老年,10,8", f"{prefix}{i},少年,12,8"]
    return ("\n".join(lines) + "\n").encode()


def csv_b(prefix="西", n=3):
    return csv_a(prefix, n)


def db_cells(db_path):
    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute(
            "SELECT survey_token, district, age_group, population, detected "
            "FROM cells ORDER BY district, age_group"
        ).fetchall()
    finally:
        conn.close()
    return rows


def db_surveys(db_path):
    conn = sqlite3.connect(db_path)
    try:
        return conn.execute("SELECT id, token FROM survey").fetchall()
    finally:
        conn.close()


# ----------------------------------------------------------------


def test_two_process_import_race():
    tmp = tempfile.mkdtemp(prefix="cdc_race_")
    db = str(Path(tmp) / "app.db")
    with Servers(db) as srv:
        A, B = csv_a(), csv_b()
        barrier = threading.Barrier(2)
        out = {}

        def go(name, port, data):
            barrier.wait()
            out[name] = post_file(port, "/api/import", data, f"{name}.csv")

        ts = [
            threading.Thread(target=go, args=("A", srv.ports[0], A)),
            threading.Thread(target=go, args=("B", srv.ports[1], B)),
        ]
        [t.start() for t in ts]
        [t.join() for t in ts]

        codes = sorted(v[0] for v in out.values())
        check("双进程并发导入：恰好一个 201、一个 409", codes == [201, 409], out)

        rows = db_cells(db)
        check("库中格数 = 获胜文件格数（9），未拼成 18 格", len(rows) == 9, len(rows))
        tokens = {r[0] for r in rows}
        check("库中只有一份获胜文件的单一 token", len(tokens) == 1, tokens)
        districts = {r[1] for r in rows}
        check(
            "库中不混入另一份文件的分区",
            districts in ({f"东{i}" for i in range(3)}, {f"西{i}" for i in range(3)}),
            districts,
        )
        surveys = db_surveys(db)
        check("survey 表只有一行获胜调查", len(surveys) == 1, surveys)

        winner_name = "A" if out["A"][0] == 201 else "B"
        check(
            f"获胜响应 201 属于{winner_name}，落败方响应为已导入冲突",
            (out[winner_name][0] == 201)
            and ("已导入" in out["B" if winner_name == "A" else "A"][1]),
            out,
        )

        # 两个实例读到的获胜调查一致，且只读获胜文件。
        st_a = json.loads(http("GET", f"http://127.0.0.1:{srv.ports[0]}/api/admin/state")[1])
        st_b = json.loads(http("GET", f"http://127.0.0.1:{srv.ports[1]}/api/admin/state")[1])
        check(
            "两个实例状态一致：同一 survey_id、同一分区集合",
            st_a["survey_id"] == st_b["survey_id"]
            and st_a["districts"] == st_b["districts"]
            and len(st_a["cells"]) == 9,
            (st_a, st_b),
        )
        pub_a = json.loads(http("GET", f"http://127.0.0.1:{srv.ports[0]}/api/public")[1])
        pub_b = json.loads(http("GET", f"http://127.0.0.1:{srv.ports[1]}/api/public")[1])
        check(
            "公开页两实例一致且仅含 9 格获胜文件、无 detected",
            pub_a == pub_b
            and len(pub_a["cells"]) == 9
            and all("detected" not in c for c in pub_a["cells"]),
            (pub_a, pub_b),
        )
        # 预览只针对获胜文件：另一份文件的分区必须视为未知。
        losing_prefix = "西" if winner_name == "A" else "东"
        status, text = post_json(
            srv.ports[0], "/api/preview",
            {"districts": [f"{losing_prefix}0"], "age_groups": ["青年"]},
        )
        check("预览拒绝另一份（落败）文件的分区，未混入其数据", status == 400, text)


def test_same_process_import_race():
    """同一进程两个请求并发导入（TestClient 之外的 ASGI 并发）。"""
    tmp = tempfile.mkdtemp(prefix="cdc_race1p_")
    db = str(Path(tmp) / "app.db")
    with Servers(db, n=1) as srv:
        port = srv.ports[0]
        barrier = threading.Barrier(2)
        out = {}

        def go(name, data):
            barrier.wait()
            out[name] = post_file(port, "/api/import", data, f"{name}.csv")

        ts = [
            threading.Thread(target=go, args=("A", csv_a())),
            threading.Thread(target=go, args=("B", csv_b())),
        ]
        [t.start() for t in ts]
        [t.join() for t in ts]
        codes = sorted(v[0] for v in out.values())
        check("同进程并发导入：恰好一个 201、一个 409", codes == [201, 409], out)
        check("同进程并发后库中只有 9 格", len(db_cells(db)) == 9, len(db_cells(db)))


def test_repeated_races():
    """多轮并发：每一轮都必须一胜一败，库中 9 格。"""
    ok = True
    detail = []
    for i in range(5):
        tmp = tempfile.mkdtemp(prefix=f"cdc_rep_{i}_")
        db = str(Path(tmp) / "app.db")
        with Servers(db) as srv:
            barrier = threading.Barrier(2)
            out = {}

            def go(name, port, data):
                barrier.wait()
                out[name] = post_file(port, "/api/import", data, f"{name}.csv")

            ts = [
                threading.Thread(target=go, args=("A", srv.ports[0], csv_a())),
                threading.Thread(target=go, args=("B", srv.ports[1], csv_b())),
            ]
            [t.start() for t in ts]
            [t.join() for t in ts]
            codes = sorted(v[0] for v in out.values())
            n = len(db_cells(db))
            if codes != [201, 409] or n != 9:
                ok = False
                detail.append((i, codes, n))
    check("连续 5 轮双进程并发均一胜一败且恰好 9 格", ok, detail)


def test_partial_write_rollback():
    """导入事务中途异常：不留任何格值或 survey 行。"""
    tmp = tempfile.mkdtemp(prefix="cdc_partial_")
    db = str(Path(tmp) / "app.db")
    code = f"""
import os, sys
sys.path.insert(0, {str(ROOT)!r})
os.environ['DB_PATH'] = {db!r}
from app import database as d
d.init_db()
rows = [dict(district=f'D{{i}}', age_group='青', population=10, detected=1) for i in range(9)]
class Boom(Exception): pass
def bad(conn, token, rows):
    # 先真写入前 4 行，再抛错：验证事务整体回滚而非只回滚未执行部分
    conn.executemany(
        'INSERT INTO cells VALUES (:survey_token, :district, :age_group, :population, :detected)',
        [dict(r, survey_token=token) for r in rows[:4]],
    )
    raise Boom('injected failure after 4 rows')
d._insert_cell_rows = bad
try:
    d.insert_survey(rows)
    print('NO_ERROR')
except Boom:
    print('ROLLED_BACK')
print('cells', len(d.get_cells()))
print('has_survey', d.get_active_survey() is not None)
"""
    out = subprocess.run([PY, "-c", code], capture_output=True, text=True, timeout=60)
    text = out.stdout + out.stderr
    check(
        "写库中途异常：抛错、格数为 0、无 survey 残留",
        "ROLLED_BACK" in text and "cells 0" in text and "has_survey False" in text,
        text,
    )

    # 进程「崩溃」打断：os._exit 跳过一切清理，模拟 kill -9。
    # 未提交事务由 SQLite 日志在下次打开时回滚，不留部分格值。
    tmp2 = tempfile.mkdtemp(prefix="cdc_kill_")
    db2 = str(Path(tmp2) / "app.db")
    code2 = f"""
import os, sys
sys.path.insert(0, {str(ROOT)!r})
os.environ['DB_PATH'] = {db2!r}
from app import database as d
d.init_db()
rows = [dict(district=f'K{{i}}', age_group='青', population=10, detected=1) for i in range(12)]
conn = d.connect()
conn.execute('BEGIN IMMEDIATE')
conn.execute("INSERT INTO survey (id, token, imported_at) VALUES (1, 'killtest', 'now')")
for r in rows[:6]:
    conn.execute("INSERT INTO cells VALUES ('killtest', :district, :age_group, :population, :detected)", r)
print('PARTIAL_WRITTEN', flush=True)
os._exit(9)   # 未 COMMIT：模拟进程崩溃
"""
    proc = subprocess.Popen(
        [PY, "-c", code2], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True
    )
    line = proc.stdout.readline()
    proc.wait(timeout=10)
    check("崩溃模拟子进程以非零码退出", proc.returncode == 9, proc.returncode)
    conn = sqlite3.connect(db2)
    try:
        n_cells = conn.execute("SELECT COUNT(*) FROM cells").fetchone()[0]
        n_survey = conn.execute("SELECT COUNT(*) FROM survey").fetchone()[0]
    finally:
        conn.close()
    check(
        "进程 kill -9 打断未提交导入：库中无部分格值与半条调查",
        "PARTIAL_WRITTEN" in line and n_cells == 0 and n_survey == 0,
        f"line={line!r} cells={n_cells} survey={n_survey}",
    )


def test_cross_process_publish_race():
    """两个进程并发发布两个各自安全、合并不安全的窗口：不能都成功。"""
    tmp = tempfile.mkdtemp(prefix="cdc_pubrace_")
    db = str(Path(tmp) / "app.db")
    with Servers(db) as srv:
        # 示例数据：城东/青年=2 为受保护小样本格。
        with open(ROOT / "sample_survey.csv", "rb") as f:
            sample = f.read()
        status, text = post_file(srv.ports[0], "/api/import", sample, "s.csv")
        check("发布竞态前导入示例成功", status == 201, text)

        # 窗口1：城东 × (青年,老年)，总数 10；窗口2：老年 × (城东,城西)，总数 16。
        # 各自安全，合并不安全（城东/青年被钉到 [0,4]）。
        windows = [
            (srv.ports[0], {"districts": ["城东"], "age_groups": ["青年", "老年"]}),
            (srv.ports[1], {"districts": ["城东", "城西"], "age_groups": ["老年"]}),
        ]
        # 先确认两者单独预览都安全。
        safe0 = json.loads(post_json(srv.ports[0], "/api/preview", windows[0][1])[1])["safe"]
        safe1 = json.loads(post_json(srv.ports[1], "/api/preview", windows[1][1])[1])["safe"]
        check("两窗口单独预览均安全", safe0 and safe1, (safe0, safe1))

        barrier = threading.Barrier(2)
        out = {}

        def go(i, port, payload):
            barrier.wait()
            out[i] = post_json(port, "/api/publish", payload)

        ts = [threading.Thread(target=go, args=(i, p, w)) for i, (p, w) in enumerate(windows)]
        [t.start() for t in ts]
        [t.join() for t in ts]

        codes = sorted(v[0] for v in out.values())
        check("两进程并发发布：不能都成功（一个 201、一个 409）", codes == [201, 409], out)
        conn = sqlite3.connect(db)
        try:
            n_pub = conn.execute("SELECT COUNT(*) FROM publications").fetchone()[0]
        finally:
            conn.close()
        check("库中只有一条公开记录（合并后不安全的窗口被拒）", n_pub == 1, n_pub)
        loser = out[0] if out[0][0] == 409 else out[1]
        detail = json.loads(loser[1]).get("detail", {})
        check(
            "落败方 409 含受影响格 城东/青年 且可行上限 4",
            isinstance(detail, dict)
            and detail.get("affected")
            and detail["affected"][0]["district"] == "城东"
            and detail["affected"][0]["age_group"] == "青年"
            and detail["affected"][0]["max"] == 4,
            loser,
        )


def test_legacy_migration():
    """旧版无 survey_token 的库可自动迁移并保留数据。"""
    tmp = tempfile.mkdtemp(prefix="cdc_legacy_")
    db = str(Path(tmp) / "app.db")
    conn = sqlite3.connect(db)
    conn.executescript(
        """
        CREATE TABLE cells (
            district TEXT NOT NULL, age_group TEXT NOT NULL,
            population INTEGER NOT NULL, detected INTEGER NOT NULL,
            PRIMARY KEY (district, age_group)
        );
        CREATE TABLE publications (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            districts TEXT NOT NULL, age_groups TEXT NOT NULL,
            total INTEGER NOT NULL, created_at TEXT NOT NULL
        );
        INSERT INTO cells VALUES ('旧区','青',10,2),('旧区','老',10,8);
        INSERT INTO publications (districts, age_groups, total, created_at)
            VALUES ('["旧区"]','["青","老"]',10,'2024-01-01T00:00:00+00:00');
        """
    )
    conn.commit()
    conn.close()
    code = f"""
import os, sys
sys.path.insert(0, {str(ROOT)!r})
os.environ['DB_PATH'] = {db!r}
from app import database as d
d.init_db()
s = d.get_active_survey()
cells = d.get_cells()
pubs = d.get_publications()
print(s is not None, len(cells), len(pubs), pubs[0]['total'] if pubs else None)
"""
    out = subprocess.run([PY, "-c", code], capture_output=True, text=True, timeout=60)
    check(
        "旧库自动迁移：保留 2 格与 1 条公开记录",
        out.stdout.strip() == "True 2 1 10",
        out.stdout + out.stderr,
    )


def test_demo_pair_endpoint():
    """演示页接口：向两个实例并发导入两份 CSV，返回一胜一败与一致状态。"""
    tmp = tempfile.mkdtemp(prefix="cdc_demo_")
    db = str(Path(tmp) / "app.db")
    with Servers(db, extra_env={"DEMO_MODE": "1"}) as srv:
        port = srv.ports[0]
        a = (ROOT / "sample_survey.csv").read_bytes()
        b = (ROOT / "sample_survey_b.csv").read_bytes()
        boundary = "----cdcdemo"
        body = (
            f"--{boundary}\r\n"
            'Content-Disposition: form-data; name="file_a"; filename="a.csv"\r\n'
            "Content-Type: text/csv\r\n\r\n"
        ).encode() + a
        body += (
            f"\r\n--{boundary}\r\n"
            'Content-Disposition: form-data; name="file_b"; filename="b.csv"\r\n'
            "Content-Type: text/csv\r\n\r\n"
        ).encode() + b
        body += f"\r\n--{boundary}--\r\n".encode()
        status, text = http(
            "POST",
            f"http://127.0.0.1:{port}/api/demo/import-pair",
            body=body,
            headers={"Content-Type": f"multipart/form-data; boundary={boundary}"},
            timeout=60,
        )
        check("演示接口返回 200", status == 200, text)
        j = json.loads(text)
        check(
            "演示接口：恰好一胜一败",
            j["winner_count"] == 1 and j["conflict_count"] == 1,
            j,
        )
        check(
            "演示接口：两实例看到同一获胜调查且只保留 4 格",
            len(j["states"]) == 2
            and all(s.get("cell_count") == 4 for s in j["states"])
            and len({s.get("survey_id") for s in j["states"]}) == 1
            and j["db"]["cell_count"] == 4,
            j,
        )
        check(
            "演示接口：获胜文件分区完整、未混入另一份",
            set(j["db"]["districts"]) in (
                {"城东", "城西"},
                {"城南", "城北"},
            ),
            j["db"],
        )
        status, text = http("POST", f"http://127.0.0.1:{port}/api/demo/reset", body=b"")
        check("演示模式可重置", status == 200, text)
        check("重置后库中无格值", len(db_cells(db)) == 0, len(db_cells(db)))
        status, text = http("GET", f"http://127.0.0.1:{port}/demo", timeout=10)
        check("演示页可访问且含并发说明", status == 200 and "并发导入" in text, status)


def test_concurrent_cold_start():
    """两个服务进程同时首次启动：WAL 切换竞态不应导致启动失败。"""
    ok = True
    detail = []
    for i in range(3):
        tmp = tempfile.mkdtemp(prefix=f"cdc_cold_{i}_")
        db = str(Path(tmp) / "app.db")
        try:
            with Servers(db) as srv:
                st_a = http("GET", f"http://127.0.0.1:{srv.ports[0]}/api/public")[0]
                st_b = http("GET", f"http://127.0.0.1:{srv.ports[1]}/api/public")[0]
                if not (st_a == 200 and st_b == 200):
                    ok = False
                    detail.append((i, st_a, st_b))
        except RuntimeError as e:
            ok = False
            detail.append((i, str(e)))
    check("两个进程同时冷启动均成功（WAL 切换无竞态）", ok, detail)


def main():
    test_two_process_import_race()
    test_same_process_import_race()
    test_repeated_races()
    test_concurrent_cold_start()
    test_partial_write_rollback()
    test_cross_process_publish_race()
    test_legacy_migration()
    test_demo_pair_endpoint()
    print(f"\n{sum(results)}/{len(results)} 通过")
    return 0 if all(results) else 1


if __name__ == "__main__":
    sys.exit(main())
