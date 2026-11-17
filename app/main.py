"""FastAPI 应用：疾控汇总发布安全审查台。

页面：
- GET /        内部审查台（导入、预览、发布；可见原始格值）
- GET /public  公开页（仅人数与获准汇总，无格内检出数）
- GET /demo    并发导入演示页（两份 CSV 同时导入两个服务实例）

接口：
- POST /api/import        导入调查 CSV（全局仅允许一次，导入后不可更换）
- GET  /api/admin/state   内部状态（含格内检出数）
- POST /api/preview       预览交集检出总数并做安全评估
- POST /api/publish       确认发布（跨进程写锁内重核后原子写入）
- GET  /api/public        公开数据（人数 + 获准汇总）
- GET  /api/demo/config   演示配置（自身/对端地址）
- POST /api/demo/import-pair  向两个实例并发导入两份 CSV 并汇总结果
- POST /api/demo/reset    仅 DEMO_MODE 下可用的演示数据重置
"""

import asyncio
import csv
import io
import os
import sqlite3
from contextlib import asynccontextmanager
from pathlib import Path

import httpx
from fastapi import FastAPI, File, HTTPException, Request, UploadFile
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field

from .database import (
    NoActiveSurvey,
    PublicationConflict,
    SurveyAlreadyExists,
    SurveyChanged,
    connect,
    get_active_survey,
    get_snapshot,
    init_db,
    insert_survey,
    publish_checked,
)
from .solver import evaluate

BASE_DIR = Path(__file__).resolve().parent.parent
STATIC_DIR = Path(__file__).resolve().parent / "static"
SAMPLE_CSV = BASE_DIR / "sample_survey.csv"

MAX_CELLS = 16
MIN_POPULATION = 5

# 演示模式：启用 /api/demo/reset（仅演示页使用）。默认关闭。
DEMO_MODE = os.environ.get("DEMO_MODE", "0").lower() in {"1", "true", "yes", "on"}
# 未显式配置 SELF_URL 时，演示接口按当前请求的 base_url 访问本实例。
SELF_URL = os.environ.get("SELF_URL", "").rstrip("/")
PEER_URLS = [
    u.strip().rstrip("/")
    for u in os.environ.get("PEER_URLS", "").split(",")
    if u.strip()
]
MAX_PUBLISH_RETRIES = 8


def _self_url(request: Request) -> str:
    return SELF_URL or str(request.base_url).rstrip("/")


@asynccontextmanager
async def lifespan(_: FastAPI):
    init_db()
    yield


app = FastAPI(title="疾控汇总发布安全审查台", lifespan=lifespan)

# 进程内发布串行化（跨进程由 SQLite BEGIN IMMEDIATE 保证）。
publish_lock = asyncio.Lock()


# ---------------- CSV 解析 ----------------


_HEADER_ALIASES = {
    "district": {"分区", "district", "地区", "区域"},
    "age_group": {"年龄组", "age_group", "agegroup", "age"},
    "population": {"人数", "population", "pop"},
    "detected": {"检出数", "detected", "count"},
}


def parse_survey_csv(text: str) -> list[dict]:
    reader = csv.reader(io.StringIO(text))
    rows = [r for r in reader if any(str(cell).strip() for cell in r)]
    if not rows:
        raise ValueError("CSV 为空")
    header = [str(h).strip().lstrip("﻿").lower() for h in rows[0]]
    col: dict[str, int] = {}
    for field, aliases in _HEADER_ALIASES.items():
        aliases = {a.lower() for a in aliases}
        for idx, h in enumerate(header):
            if h in aliases:
                col[field] = idx
                break
        if field not in col:
            raise ValueError(f"缺少必需列：{field}（可用表头：{sorted(aliases)}）")

    out, seen = [], set()
    for lineno, r in enumerate(rows[1:], start=2):
        try:
            district = r[col["district"]].strip()
            age = r[col["age_group"]].strip()
            pop_s = r[col["population"]].strip()
            det_s = r[col["detected"]].strip()
        except IndexError:
            raise ValueError(f"第 {lineno} 行列数不足")
        if not district or not age:
            raise ValueError(f"第 {lineno} 行分区或年龄组为空")
        try:
            pop = int(pop_s)
        except ValueError:
            raise ValueError(f"第 {lineno} 行人数不是整数：{pop_s!r}")
        try:
            det = int(det_s)
        except ValueError:
            raise ValueError(f"第 {lineno} 行检出数不是整数：{det_s!r}")
        if pop < MIN_POPULATION:
            raise ValueError(f"第 {lineno} 行人数 {pop} 小于下限 {MIN_POPULATION}")
        if not (0 <= det <= pop):
            raise ValueError(f"第 {lineno} 行检出数 {det} 不在 0~人数({pop}) 范围内")
        key = (district, age)
        if key in seen:
            raise ValueError(f"组合重复：{district} / {age}")
        seen.add(key)
        out.append(
            {"district": district, "age_group": age, "population": pop, "detected": det}
        )
    if not out:
        raise ValueError("没有数据行")
    if len(out) > MAX_CELLS:
        raise ValueError(f"格数 {len(out)} 超过上限 {MAX_CELLS}")
    return out


def _decode_csv(raw: bytes) -> list[dict]:
    try:
        text = raw.decode("utf-8-sig")
    except UnicodeDecodeError:
        raise HTTPException(400, "文件编码须为 UTF-8")
    try:
        return parse_survey_csv(text)
    except ValueError as e:
        raise HTTPException(400, str(e))


# ---------------- 请求模型 ----------------


class Selection(BaseModel):
    districts: list[str] = Field(default_factory=list)
    age_groups: list[str] = Field(default_factory=list)


# ---------------- 工具 ----------------


def _match(cells: list[dict], sel: Selection) -> tuple[list[dict], int]:
    known_d = {c["district"] for c in cells}
    known_a = {c["age_group"] for c in cells}
    bad_d = [d for d in sel.districts if d not in known_d]
    bad_a = [a for a in sel.age_groups if a not in known_a]
    if bad_d or bad_a:
        raise HTTPException(400, f"未知的分区或年龄组：{bad_d + bad_a}")
    ds, ags = set(sel.districts), set(sel.age_groups)
    matched = [c for c in cells if c["district"] in ds and c["age_group"] in ags]
    if not matched:
        raise HTTPException(400, "选择为空或交集不含任何格")
    return matched, sum(c["detected"] for c in matched)


# ---------------- 页面 ----------------


@app.get("/", include_in_schema=False)
def index_page():
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/public", include_in_schema=False)
def public_page():
    return FileResponse(STATIC_DIR / "public.html")


@app.get("/demo", include_in_schema=False)
def demo_page():
    return FileResponse(STATIC_DIR / "demo.html")


@app.get("/sample_survey.csv", include_in_schema=False)
def sample_csv():
    return FileResponse(SAMPLE_CSV, media_type="text/csv")


@app.get("/sample_survey_b.csv", include_in_schema=False)
def sample_csv_b():
    return FileResponse(BASE_DIR / "sample_survey_b.csv", media_type="text/csv")


# ---------------- 接口 ----------------


@app.post("/api/import", status_code=201)
async def import_csv(file: UploadFile = File(...)):
    # 快速失败：已存在获胜调查时直接 409（权威判定仍在 insert_survey 的事务内）。
    if get_active_survey() is not None:
        raise HTTPException(409, "调查数据已导入，不可更换")
    rows = _decode_csv(await file.read())
    try:
        # 解析与写库放到工作线程：同一进程的并发导入也能真实交错，
        # 由 SQLite 写锁决定唯一获胜方。
        survey = await asyncio.to_thread(insert_survey, rows)
    except SurveyAlreadyExists:
        raise HTTPException(409, "调查数据已导入，不可更换")
    except sqlite3.OperationalError as e:
        raise HTTPException(503, f"数据库繁忙，导入未写入任何数据：{e}")
    return {
        "imported": len(rows),
        "districts": sorted({r["district"] for r in rows}),
        "age_groups": sorted({r["age_group"] for r in rows}),
        "survey_id": survey["token"][:12],
    }


@app.get("/api/admin/state")
def admin_state():
    snap = get_snapshot()
    cells = snap["cells"]
    survey = snap["survey"]
    return {
        "imported": bool(cells) and survey is not None,
        "survey_id": survey["token"][:12] if survey else None,
        "districts": sorted({c["district"] for c in cells}),
        "age_groups": sorted({c["age_group"] for c in cells}),
        "cells": cells,  # 内部台可见原始检出数
        "publications": snap["publications"],
    }


def _candidate_result(cells, pubs, sel, total) -> dict:
    candidate = {
        "districts": list(sel.districts),
        "age_groups": list(sel.age_groups),
        "total": total,
    }
    return evaluate(cells, pubs, candidate)


@app.post("/api/preview")
async def preview(sel: Selection):
    snap = await asyncio.to_thread(get_snapshot)
    cells = snap["cells"]
    if not cells or snap["survey"] is None:
        raise HTTPException(400, "尚未导入调查数据")
    matched, total = _match(cells, sel)
    # 求解放入工作线程，求解期间公开页与其他请求不受影响。
    result = await asyncio.to_thread(
        _candidate_result, cells, snap["publications"], sel, total
    )
    return {"total": total, "cells_matched": len(matched), "survey_id": snap["survey"]["token"][:12], **result}


@app.post("/api/publish", status_code=201)
async def publish(sel: Selection):
    """确认发布：跨进程写锁内重读最新公开记录并重核，随后同事务写入。

    若临界区内发现公开记录或调查已变化，按新快照重新评估后重试，
    因此不会用过期结论放行；最终仍不安全则 409。
    """
    for attempt in range(MAX_PUBLISH_RETRIES):
        snap = await asyncio.to_thread(get_snapshot)
        cells = snap["cells"]
        if not cells or snap["survey"] is None:
            raise HTTPException(400, "尚未导入调查数据")
        survey_token = snap["survey"]["token"]
        _, total = _match(cells, sel)

        def approve(
            lock_cells: list[dict],
            lock_pubs: list[dict],
            sel=sel,
            total=total,
        ) -> None:
            result = _candidate_result(lock_cells, lock_pubs, sel, total)
            if not result["safe"]:
                raise HTTPException(
                    status_code=409,
                    detail={
                        "message": "按最新公开记录重核未通过：可能暴露小样本格值，已拒绝发布",
                        "affected": result["affected"],
                    },
                )

        # 进程内锁避免本进程请求挤在 SQLite 写锁上；跨进程由 BEGIN IMMEDIATE 串行。
        async with publish_lock:
            try:
                return await asyncio.to_thread(
                    publish_checked,
                    sel.districts,
                    sel.age_groups,
                    total,
                    survey_token,
                    len(snap["publications"]),
                    approve,
                )
            except (PublicationConflict, SurveyChanged):
                # 另一进程刚发布：按新快照重读重核后重试，绝不用过期结论放行。
                if attempt == MAX_PUBLISH_RETRIES - 1:
                    raise HTTPException(503, "发布冲突过多，请重试")
            except NoActiveSurvey:
                raise HTTPException(400, "尚未导入调查数据")
            except HTTPException:
                raise
            except sqlite3.OperationalError as e:
                raise HTTPException(503, f"数据库繁忙，发布未写入：{e}")
            except Exception:
                # publish_checked 内事务已整体回滚：不会留下半次发布。
                raise HTTPException(500, "保存失败，未产生任何发布记录")
        await asyncio.sleep(0.01 * (attempt + 1))

    raise HTTPException(503, "发布冲突过多，请重试")


@app.get("/api/public")
def public_data():
    """公开数据：仅人数与获准汇总，绝不包含格内检出数。"""
    snap = get_snapshot()
    cells = snap["cells"]
    survey = snap["survey"]
    return {
        "imported": bool(cells) and survey is not None,
        "survey_id": survey["token"][:12] if survey else None,
        "cells": [
            {
                "district": c["district"],
                "age_group": c["age_group"],
                "population": c["population"],
            }
            for c in cells
        ],
        "publications": snap["publications"],
    }


# ---------------- 并发导入演示 ----------------


@app.get("/api/demo/config")
def demo_config(request: Request):
    self_url = _self_url(request)
    return {
        "demo_mode": DEMO_MODE,
        "self": self_url,
        "peers": PEER_URLS,
        "targets": [self_url, *PEER_URLS],
    }


async def _post_import(client: httpx.AsyncClient, target: str, name: str, raw: bytes):
    try:
        r = await client.post(
            f"{target}/api/import",
            files={"file": (name, raw, "text/csv")},
        )
        try:
            body = r.json()
        except ValueError:
            body = {"raw": r.text}
        return {"target": target, "file": name, "status": r.status_code, "body": body}
    except httpx.HTTPError as e:
        return {
            "target": target,
            "file": name,
            "status": None,
            "body": {"error": f"无法连接：{e}"},
        }


@app.post("/api/demo/import-pair")
async def demo_import_pair(
    request: Request,
    file_a: UploadFile = File(..., alias="file_a"),
    file_b: UploadFile = File(..., alias="file_b"),
):
    """同时向两个实例导入两份 CSV，返回双方响应与最终库状态。

    两份请求各自发往不同目标（本地实例 + PEER_URLS）；只配置一个目标时，
    两份都发给该实例，用于演示同一进程内的并发导入。
    """
    raw_a = await file_a.read()
    raw_b = await file_b.read()
    targets = [_self_url(request), *PEER_URLS]
    target_a = targets[0]
    target_b = targets[1] if len(targets) > 1 else targets[0]

    # 先做本地校验，避免把明显非法的文件发出去；不写任何库。
    _decode_csv(raw_a)
    _decode_csv(raw_b)

    async with httpx.AsyncClient(timeout=30.0) as client:
        res_a, res_b = await asyncio.gather(
            _post_import(client, target_a, file_a.filename or "a.csv", raw_a),
            _post_import(client, target_b, file_b.filename or "b.csv", raw_b),
        )

    snap = await asyncio.to_thread(get_snapshot)
    cells = snap["cells"]
    # 从两个实例分别读取状态，证明它们看到同一份获胜调查。
    states = []
    async with httpx.AsyncClient(timeout=10.0) as client:
        for target in targets:
            try:
                r = await client.get(f"{target}/api/admin/state")
                j = r.json()
                states.append(
                    {
                        "target": target,
                        "status": r.status_code,
                        "survey_id": j.get("survey_id"),
                        "cell_count": len(j.get("cells", [])),
                        "districts": j.get("districts", []),
                    }
                )
            except (httpx.HTTPError, ValueError) as e:
                states.append({"target": target, "status": None, "error": str(e)})

    return {
        "results": [res_a, res_b],
        "winner_count": sum(1 for r in [res_a, res_b] if r["status"] == 201),
        "conflict_count": sum(1 for r in [res_a, res_b] if r["status"] == 409),
        "db": {
            "survey_id": snap["survey"]["token"][:12] if snap["survey"] else None,
            "cell_count": len(cells),
            "districts": sorted({c["district"] for c in cells}),
            "age_groups": sorted({c["age_group"] for c in cells}),
            "publications": len(snap["publications"]),
            "checksum": sum(c["detected"] for c in cells),
        },
        "states": states,
    }


@app.post("/api/demo/reset")
def demo_reset():
    """仅演示模式可用：清空调查与发布，便于反复演示。单事务。"""
    if not DEMO_MODE:
        raise HTTPException(403, "演示重置仅在 DEMO_MODE=1 时可用")

    conn = connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute("DELETE FROM publications")
        conn.execute("DELETE FROM cells")
        conn.execute("DELETE FROM survey")
        conn.execute("COMMIT")
    except BaseException:
        try:
            conn.execute("ROLLBACK")
        except sqlite3.Error:
            pass
        raise
    finally:
        conn.close()
    return {"reset": True}
