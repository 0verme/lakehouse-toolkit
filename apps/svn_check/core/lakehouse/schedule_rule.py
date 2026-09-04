"""公开版调度 Excel 结构与依赖规则。"""

from __future__ import annotations

import time
from importlib import import_module

from services.re_service import ifmiaoshu

from core.public_data import (
    all_job,
    all_job_dependencies,
    all_job_outfile,
    all_plan,
    all_planjob,
    all_planseq,
    all_real_seq,
    all_recv_mapping_plans,
    all_seq,
    all_seqjob,
    get_job2,
)
from shared.graph.dependency import find_job_dependency_cycles

pd = import_module("pandas")


ALLOWED_DOMAINS = {"DEMO_DOMAIN", "BATCH_DOMAIN", "STREAM_DOMAIN"}


def _normalize(value):
    return "" if value is None or pd.isna(value) else str(value).strip()


def _normalized_upper(value):
    return _normalize(value).upper()


def _collect_online_job_dependencies(df):
    return {
        _normalized_upper(row.iloc[2]): _normalized_upper(row.iloc[27])
        for _, row in df.iterrows()
        if _normalized_upper(row.iloc[2])
    }


def _find_online_job_dependency_cycles(df, job_rows=None):
    online_jobs = _collect_online_job_dependencies(df)
    if not online_jobs:
        return []
    merged_jobs = {}
    for row in job_rows if job_rows is not None else (all_job_dependencies() or []):
        if len(row) < 2:
            continue
        job_name = _normalized_upper(
            row[2] if job_rows is not None and len(row) > 2 else row[0]
        )
        dependency = _normalized_upper(
            row[27] if job_rows is not None and len(row) > 27 else row[1]
        )
        if job_name:
            merged_jobs[job_name] = dependency
    merged_jobs.update(online_jobs)
    return find_job_dependency_cycles(
        merged_jobs.items(), only_nodes=set(online_jobs), max_cycles=20
    )


def rule_excle_plan(df):
    result_text = ""
    warn_result_text = ""
    count = 0
    registered_plans = {
        _normalize(row[0]) for row in (all_plan() or []) if row and _normalize(row[0])
    }
    receive_plans = {
        _normalize(row[0])
        for row in (all_recv_mapping_plans() or [])
        if row and _normalize(row[0])
    }
    imported_plans = []
    for _, row in df.iterrows():
        plan_name = _normalize(row.iloc[0])
        dependency = _normalize(row.iloc[1])
        if not plan_name:
            continue
        imported_plans.append(plan_name)
        if dependency:
            result_text += f"计划名: {plan_name} 存在前置依赖 {dependency}，请检查\n"
            count += 1
        if plan_name.startswith("PLAN_EXPORT_") and plan_name not in receive_plans:
            warn_result_text += f"计划名 {plan_name} 未在接入计划映射中配置\n"
        if (
            registered_plans
            and plan_name not in registered_plans
            and not plan_name.startswith("DEMO_")
        ):
            result_text += f"计划名: {plan_name} 未在已注册调度中，请检查\n"
            count += 1
    return (
        result_text,
        warn_result_text,
        count,
        list(dict.fromkeys(imported_plans + sorted(registered_plans))),
    )


def rule_excle_seq(df):
    real_sequences = {
        _normalize(row[0])
        for row in (all_real_seq() or [])
        if row and _normalize(row[0])
    }
    result_text = ""
    count = 0
    for _, row in df.iterrows():
        sequence_name = _normalize(row.iloc[1])
        if sequence_name in real_sequences:
            result_text += (
                f"作业流名：{sequence_name} 已注册为实时作业，请确认是否重复配置\n"
            )
            count += 1
    return result_text, "", count


def rule_excle_job(df, r_plan=None, timing_log=None, job_rows=None):
    def log_timing(message):
        if timing_log:
            timing_log(message)

    started = time.time()
    outfile_map = {
        _normalize(row[0]): _normalize(row[1])
        for row in (all_job_outfile() or [])
        if len(row) >= 2 and _normalize(row[0])
    }
    log_timing(
        f"JOB 结果路径查询完成：{len(outfile_map)} 行，{time.time() - started:.2f}s"
    )

    planseq = {}
    seqjob = {}
    planjob = {}
    sequence_names = set()
    for row in job_rows if job_rows is not None else (all_job() or []):
        if len(row) > 2:
            planseq[_normalize(row[1])] = _normalize(row[0])
            seqjob[_normalize(row[2])] = _normalize(row[1])
            planjob[_normalize(row[2])] = _normalize(row[0])
            sequence_names.add(_normalize(row[1]))
    if job_rows is None:
        for row in all_planseq() or []:
            if len(row) >= 2:
                planseq[_normalize(row[1])] = _normalize(row[0])
        for row in all_seqjob() or []:
            if len(row) >= 2:
                seqjob[_normalize(row[1])] = _normalize(row[0])
        for row in all_planjob() or []:
            if len(row) >= 2:
                planjob[_normalize(row[1])] = _normalize(row[0])
        sequence_names.update(
            _normalize(row[0]) for row in all_seq() or [] if row and _normalize(row[0])
        )

    registered_status = {}
    for row in job_rows if job_rows is not None else (get_job2() or []):
        if len(row) < 2:
            continue
        job_name = _normalized_upper(
            row[2] if job_rows is not None and len(row) > 2 else row[0]
        )
        status = _normalize(
            row[23] if job_rows is not None and len(row) > 23 else row[1]
        )
        if job_name:
            registered_status[job_name] = (
                "禁用" if status in {"9", "9.0", "DISABLED"} else status
            )

    plans = list(r_plan or [])
    result_text = ""
    warn_result_text = ""
    count = 0
    for cycle in _find_online_job_dependency_cycles(df, job_rows):
        result_text += f"作业依赖成环，请检查: {' -> '.join(cycle)}\n"
        count += 1

    known_jobs = set(registered_status) | {
        _normalized_upper(row.iloc[2])
        for _, row in df.iterrows()
        if _normalized_upper(row.iloc[2])
    }
    for _, row in df.iterrows():
        plan_name = _normalize(row.iloc[0])
        sequence_name = _normalize(row.iloc[1])
        job_name = _normalize(row.iloc[2])
        description = _normalize(row.iloc[3])
        domain = _normalize(row.iloc[5]) if len(row) > 5 else ""
        priority = _normalize(row.iloc[6]) if len(row) > 6 else ""
        event_text = _normalize(row.iloc[25]) if len(row) > 25 else ""
        dependency_text = _normalize(row.iloc[27]) if len(row) > 27 else ""
        if not job_name:
            continue
        if job_name.upper() != job_name:
            result_text += f"作业名: {job_name} 不应包含小写\n"
            count += 1
        if plan_name.upper() != plan_name:
            result_text += f"计划名: {plan_name} 不应包含小写\n"
            count += 1
        if sequence_name.upper() != sequence_name:
            result_text += f"作业流名: {sequence_name} 不应包含小写\n"
            count += 1
        if registered_status.get(job_name.upper()) == "禁用":
            result_text += f"作业名：{job_name} 当前为禁用状态，请确认\n"
            count += 1
        if not sequence_name:
            result_text += f"作业流名为空: {job_name}\n"
            count += 1
        if "REAL" in sequence_name.upper() and priority not in {"99", "99.0"}:
            result_text += f"{job_name} 实时作业优先级应为 99\n"
            count += 1
        if not description or ifmiaoshu(description, job_name):
            result_text += f"{job_name} 缺少清晰的作业描述\n"
            count += 1
        if domain and domain.upper() not in ALLOWED_DOMAINS:
            result_text += f"{job_name} 使用了未登记执行域: {domain}\n"
            count += 1
        if (
            plan_name
            and plans
            and plan_name not in plans
            and not plan_name.startswith("DEMO_")
        ):
            warn_result_text += f"{plan_name} 未在本次调度清单中出现\n"
        if dependency_text and "：" in dependency_text:
            result_text += f"{job_name} 依赖列存在中文冒号，请修改\n"
            count += 1
        for item in dependency_text.split("|"):
            parts = item.split(":", 1)
            dependency = _normalize(parts[1] if len(parts) == 2 else "")
            if not dependency:
                continue
            if dependency == job_name:
                result_text += f"{job_name} 不应依赖自身\n"
                count += 1
            elif dependency not in known_jobs:
                warn_result_text += (
                    f"{job_name} 的前置作业 {dependency} 未在当前元数据中找到\n"
                )
        if event_text and "\t" in event_text:
            result_text += f"{job_name} 参数含 TAB，请修改\n"
            count += 1
        if event_text and "  " in event_text:
            warn_result_text += f"{job_name} 参数含连续空格，请确认\n"
        if event_text and "outfile=" in event_text:
            output_name = outfile_map.get(
                event_text.split("outfile=", 1)[1].split(":", 1)[0]
            )
            if output_name and output_name != job_name:
                warn_result_text += f"{job_name} 与已有输出路径可能重复，请确认\n"
    return result_text, warn_result_text, count
