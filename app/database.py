"""SQLite 持久化层。

数据模型
--------
- survey：全局唯一的「已导入调查文件」身份（id 恒为 1）。
  首次导入时，survey 行与全部 cells 行在**同一个 IMMEDIATE 事务**内提交，
  因此无论并发请求来自同一进程还是共享同一 SQLite 文件的多个进程，
  都只有一个文件能成为获胜的固定调查；另一方在写锁释放后看到 survey 行，
  收到“已导入”冲突。
- cells：获胜调查的格值，带 survey_token，导入后不可更换。
- publications：基于获胜调查发布的汇总，带 survey_token。

所有读取都按当前 survey.token 过滤：即使库中残留其他 token 的行
（历史数据或人为写入），预览、公开页、发布也不会混入另一份文件的数据。

所有写操作均显式使用事务：失败/中断整体回滚，不会留下部分格值或半次发布。
连接按操作创建，启用 WAL 与 busy_timeout，冲突时等待而不是立即失败。
"""

import json
import logging
import os
import sqlite3
import time
import uuid
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path

_DEFAULT_DB = Path(__file__).resolve().parent.parent / "data" / "app.db"
DB_PATH = os.environ.get("DB_PATH", str(_DEFAULT_DB))

LEGACY_TOKEN = "legacy"
MAX_CELLS = 16

log = logging.getLogger(__name__)


class SurveyAlreadyExists(Exception):
    """已有获胜的固定调查；并发导入的落败方收到此异常。"""

    def __init__(self, token: str | None = None):
        self.token = token
        super().__init__("调查数据已导入，不可更换")


class NoActiveSurvey(Exception):
    """尚未导入任何调查。"""


class PublicationConflict(Exception):
    """发布临界区内发现公开记录已变化（其他进程刚发布），须重核后重试。"""


class SurveyChanged(Exception):
    """发布临界区内发现当前调查已变化（正常流程不会发生，调查不可更换）。"""


_SCHEMA_SURVEY = """
CREATE TABLE IF NOT EXISTS survey (
    id          INTEGER PRIMARY KEY CHECK (id = 1),
    token       TEXT NOT NULL UNIQUE,
    imported_at TEXT NOT NULL
)
"""

_SCHEMA_CELLS = """
CREATE TABLE IF NOT EXISTS cells (
    survey_token TEXT NOT NULL,
    district     TEXT NOT NULL,
    age_group    TEXT NOT NULL,
    population   INTEGER NOT NULL CHECK (population >= 5),
    detected     INTEGER NOT NULL CHECK (detected >= 0 AND detected <= population),
    PRIMARY KEY (survey_token, district, age_group),
    FOREIGN KEY (survey_token) REFERENCES survey(token) ON DELETE RESTRICT
)
"""

_SCHEMA_PUBLICATIONS = """
CREATE TABLE IF NOT EXISTS publications (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    survey_token TEXT NOT NULL,
    districts    TEXT NOT NULL,          -- JSON 数组
    age_groups   TEXT NOT NULL,          -- JSON 数组
    total        INTEGER NOT NULL CHECK (total >= 0),
    created_at   TEXT NOT NULL,          -- ISO8601 UTC
    FOREIGN KEY (survey_token) REFERENCES survey(token) ON DELETE RESTRICT
)
"""


def connect() -> sqlite3.Connection:
    path = DB_PATH
    if path != ":memory:":
        Path(path).parent.mkdir(parents=True, exist_ok=True)
    # isolation_level=None：事务边界全部显式控制。
    # timeout/busy_timeout 让并发写者等待锁，而不是立刻失败。
    conn = sqlite3.connect(path, timeout=30, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=30000")
    _ensure_wal(conn)
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def _ensure_wal(conn: sqlite3.Connection) -> None:
    """幂等启用 WAL。

    `PRAGMA journal_mode=WAL` 在切换模式时需短暂独占库，且不受 busy_timeout
    保护；多个服务进程同时首次启动时可能瞬时 "database is locked"。
    此时库已经是 WAL（或由另一进程刚切换），重试读取当前模式即可。

    内存库或不支持 WAL 的文件系统上退化为默认日志模式；跨进程互斥仍由
    BEGIN IMMEDIATE 的写锁保证，只是并发读性能略低。
    """
    if DB_PATH == ":memory:":
        return
    for attempt in range(50):
        try:
            mode = conn.execute("PRAGMA journal_mode=WAL").fetchone()[0]
            if str(mode).lower() == "wal":
                return
            break
        except sqlite3.OperationalError as e:
            if "locked" not in str(e).lower() and "busy" not in str(e).lower():
                raise
            time.sleep(0.02 * (attempt + 1))
    log.warning(
        "无法启用 WAL（当前日志模式：%s），继续使用默认日志模式；"
        "并发正确性仍由写事务保证。",
        conn.execute("PRAGMA journal_mode").fetchone()[0],
    )


def _table_exists(conn: sqlite3.Connection, name: str) -> bool:
    return (
        conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)
        ).fetchone()
        is not None
    )


def _columns(conn: sqlite3.Connection, name: str) -> set[str]:
    return {r["name"] for r in conn.execute(f"PRAGMA table_info({name})")}


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _ensure_survey_token_fk(conn: sqlite3.Connection) -> None:
    """早期迁移建出的 cells 表可能没有 FOREIGN KEY；SQLite 无法后加约束。

    这里做一次原地重建，使新库与迁移库的约束一致（数据不变）。
    """
    fks = conn.execute("PRAGMA foreign_key_list(cells)").fetchall()
    if any(fk["table"] == "survey" for fk in fks):
        return
    if conn.execute("SELECT COUNT(*) FROM cells").fetchone()[0] == 0 and not conn.execute(
        "SELECT COUNT(*) FROM survey"
    ).fetchone()[0]:
        # 空库：直接重建，避免为非空旧数据引入引用完整性问题。
        conn.execute("DROP TABLE cells")
        conn.execute(_SCHEMA_CELLS)


def _migrate_legacy(conn: sqlite3.Connection) -> None:
    """把旧版（无 survey_token）库原地升级为带文件身份的模型。

    旧库中的全部格值与公开记录归入 token=legacy 的唯一调查。
    旧版本可能已经因并发缺陷混入两份文件，SQLite 无法再拆分；
    保留原数据并记录警告，避免静默删除用户数据。
    """
    n_legacy = conn.execute("SELECT COUNT(*) FROM cells").fetchone()[0]
    if n_legacy > MAX_CELLS:
        log.warning(
            "旧库中存在 %d 格（超过单文件上限 %d），可能是旧版本并发导入遗留；"
            "已整体归入 legacy 调查，请人工核对。",
            n_legacy,
            MAX_CELLS,
        )
    if n_legacy:
        conn.execute(
            "INSERT OR IGNORE INTO survey (id, token, imported_at) VALUES (1, ?, ?)",
            (LEGACY_TOKEN, _now()),
        )
    conn.execute("ALTER TABLE cells RENAME TO cells_legacy")
    conn.execute(_SCHEMA_CELLS)
    conn.execute(
        "INSERT INTO cells (survey_token, district, age_group, population, detected) "
        "SELECT ?, district, age_group, population, detected FROM cells_legacy",
        (LEGACY_TOKEN,),
    )
    conn.execute("DROP TABLE cells_legacy")


def _migrate_legacy_publications(conn: sqlite3.Connection) -> None:
    n_legacy = conn.execute("SELECT COUNT(*) FROM publications").fetchone()[0]
    if n_legacy:
        conn.execute(
            "INSERT OR IGNORE INTO survey (id, token, imported_at) VALUES (1, ?, ?)",
            (LEGACY_TOKEN, _now()),
        )
    conn.execute(
        "ALTER TABLE publications ADD COLUMN survey_token TEXT NOT NULL DEFAULT 'legacy'"
    )


def init_db() -> None:
    """建表/迁移。使用 IMMEDIATE 写锁，多个服务进程同时启动也安全。"""
    conn = connect()
    try:
        conn.execute("BEGIN IMMEDIATE")

        conn.execute(_SCHEMA_SURVEY)

        cells_exists = _table_exists(conn, "cells")
        if cells_exists and "survey_token" not in _columns(conn, "cells"):
            _migrate_legacy(conn)
        elif not cells_exists:
            conn.execute(_SCHEMA_CELLS)
        else:
            _ensure_survey_token_fk(conn)

        pubs_exists = _table_exists(conn, "publications")
        if pubs_exists and "survey_token" not in _columns(conn, "publications"):
            _migrate_legacy_publications(conn)
        elif not pubs_exists:
            conn.execute(_SCHEMA_PUBLICATIONS)

        conn.execute("COMMIT")
    except BaseException:
        try:
            conn.execute("ROLLBACK")
        except sqlite3.Error:
            pass
        raise
    finally:
        conn.close()


# ---------------- 读取（始终按获胜调查过滤） ----------------


def get_active_survey() -> dict | None:
    with closing(connect()) as conn:
        row = conn.execute(
            "SELECT token, imported_at FROM survey WHERE id = 1"
        ).fetchone()
    return dict(row) if row else None


def get_snapshot() -> dict:
    """在一次读事务中取获胜调查、其全部格值与公开记录的一致快照。"""
    with closing(connect()) as conn:
        conn.execute("BEGIN")
        try:
            survey = conn.execute(
                "SELECT token, imported_at FROM survey WHERE id = 1"
            ).fetchone()
            if not survey:
                return {"survey": None, "cells": [], "publications": []}
            token = survey["token"]
            cells = [
                dict(r)
                for r in conn.execute(
                    "SELECT district, age_group, population, detected FROM cells "
                    "WHERE survey_token = ? ORDER BY district, age_group",
                    (token,),
                ).fetchall()
            ]
            pubs = [
                dict(r)
                for r in conn.execute(
                    "SELECT id, districts, age_groups, total, created_at "
                    "FROM publications WHERE survey_token = ? ORDER BY id",
                    (token,),
                ).fetchall()
            ]
            for p in pubs:
                p["districts"] = json.loads(p["districts"])
                p["age_groups"] = json.loads(p["age_groups"])
            return {
                "survey": {"token": token, "imported_at": survey["imported_at"]},
                "cells": cells,
                "publications": pubs,
            }
        finally:
            conn.execute("ROLLBACK")


def get_cells(token: str | None = None) -> list[dict]:
    """获胜调查的格值。token 省略时取当前获胜调查。"""
    with closing(connect()) as conn:
        if token is None:
            row = conn.execute("SELECT token FROM survey WHERE id = 1").fetchone()
            if not row:
                return []
            token = row["token"]
        rows = conn.execute(
            "SELECT district, age_group, population, detected FROM cells "
            "WHERE survey_token = ? ORDER BY district, age_group",
            (token,),
        ).fetchall()
    return [dict(r) for r in rows]


def get_publications(token: str | None = None) -> list[dict]:
    with closing(connect()) as conn:
        if token is None:
            row = conn.execute("SELECT token FROM survey WHERE id = 1").fetchone()
            if not row:
                return []
            token = row["token"]
        rows = conn.execute(
            "SELECT id, districts, age_groups, total, created_at "
            "FROM publications WHERE survey_token = ? ORDER BY id",
            (token,),
        ).fetchall()
    out = []
    for r in rows:
        d = dict(r)
        d["districts"] = json.loads(d["districts"])
        d["age_groups"] = json.loads(d["age_groups"])
        out.append(d)
    return out


# ---------------- 发布临界区 ----------------


def publish_checked(
    districts: list[str],
    age_groups: list[str],
    total: int,
    expected_survey: str,
    expected_publications: int,
    approve,
) -> dict:
    """在跨进程写锁内完成「重读最新记录 → 重核 → 写入」。

    BEGIN IMMEDIATE 使同一 SQLite 文件上的发布临界区在**所有服务进程间**
    串行：先拿到锁的请求写入并提交后，后到者才读到更新后的公开记录，
    因此两个各自安全、合并不安全的窗口不会都被发布。

    expected_survey / expected_publications 是调用方预览时的快照版本；
    若临界区内发现已变化，抛 PublicationConflict / SurveyChanged，
    由调用方按新快照重新评估（而不是用过期数据放行）。

    approve(cells, publications) 在锁内被调用；返回 None 表示通过，
    否则抛出 HTTPException 等异常，事务回滚、不产生任何发布记录。
    """
    conn = connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        survey = conn.execute("SELECT token FROM survey WHERE id = 1").fetchone()
        if not survey:
            conn.execute("ROLLBACK")
            raise NoActiveSurvey("尚未导入调查数据")
        token = survey["token"]
        if token != expected_survey:
            conn.execute("ROLLBACK")
            raise SurveyChanged()

        cells = [
            dict(r)
            for r in conn.execute(
                "SELECT district, age_group, population, detected FROM cells "
                "WHERE survey_token = ? ORDER BY district, age_group",
                (token,),
            ).fetchall()
        ]
        pub_rows = conn.execute(
            "SELECT id, districts, age_groups, total, created_at "
            "FROM publications WHERE survey_token = ? ORDER BY id",
            (token,),
        ).fetchall()
        if len(pub_rows) != expected_publications:
            conn.execute("ROLLBACK")
            raise PublicationConflict()

        pubs = []
        for r in pub_rows:
            d = dict(r)
            d["districts"] = json.loads(d["districts"])
            d["age_groups"] = json.loads(d["age_groups"])
            pubs.append(d)

        approve(cells, pubs)

        created = _now()
        cur = conn.execute(
            "INSERT INTO publications "
            "(survey_token, districts, age_groups, total, created_at) "
            "VALUES (?, ?, ?, ?, ?)",
            (
                token,
                json.dumps(list(districts), ensure_ascii=False),
                json.dumps(list(age_groups), ensure_ascii=False),
                int(total),
                created,
            ),
        )
        pid = cur.lastrowid
        conn.execute("COMMIT")
        return {
            "id": pid,
            "districts": list(districts),
            "age_groups": list(age_groups),
            "total": int(total),
            "created_at": created,
        }
    except (NoActiveSurvey, SurveyChanged, PublicationConflict):
        raise
    except BaseException:
        try:
            conn.execute("ROLLBACK")
        except sqlite3.Error:
            pass
        raise
    finally:
        conn.close()


# ---------------- 写入（单事务，失败整体回滚） ----------------


def _insert_cell_rows(
    conn: sqlite3.Connection, token: str, rows: list[dict]
) -> None:
    """写入全部格值。单独成函数便于测试注入中途失败（验证整体回滚）。"""
    conn.executemany(
        "INSERT INTO cells "
        "(survey_token, district, age_group, population, detected) "
        "VALUES (:survey_token, :district, :age_group, :population, :detected)",
        [dict(r, survey_token=token) for r in rows],
    )


def insert_survey(rows: list[dict]) -> dict:
    """首次导入：survey 身份 + 全部格值在同一 IMMEDIATE 事务内提交。

    - 并发落败方：BEGIN IMMEDIATE 等到获胜事务提交后看到 survey 行，
      抛 SurveyAlreadyExists；不会写入任何格值。
    - 写入中途异常/进程中断：事务回滚（或由 SQLite 日志恢复），不留部分格值。
    """
    token = uuid.uuid4().hex
    imported_at = _now()
    conn = connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        existing = conn.execute("SELECT token FROM survey WHERE id = 1").fetchone()
        if existing:
            conn.execute("ROLLBACK")
            raise SurveyAlreadyExists(existing["token"])
        conn.execute(
            "INSERT INTO survey (id, token, imported_at) VALUES (1, ?, ?)",
            (token, imported_at),
        )
        _insert_cell_rows(conn, token, rows)
        conn.execute("COMMIT")
        return {"token": token, "imported_at": imported_at}
    except SurveyAlreadyExists:
        raise
    except BaseException:
        try:
            conn.execute("ROLLBACK")
        except sqlite3.Error:
            pass
        raise
    finally:
        conn.close()

