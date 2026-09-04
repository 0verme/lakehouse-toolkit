# !/bin/python
import fnmatch
import os
import re
import shutil
from collections import Counter
from datetime import datetime
from pathlib import Path
from urllib.parse import quote

import pandas as pd
import xlrd

DOT_STRING_PATTERN = re.compile(r"\b\w+\.\w+\b")
TABLE_PATTERN = re.compile(r"\b(FROM|JOIN|USING)\s+([\w.]+)", re.IGNORECASE)
DATE_PATTERN = re.compile(r"'\b\d{4}[-]?\d{2}[-]?\d{2}\b'")
OUTFILE_PATTERN = re.compile(r"-outfile:\d+:(outfile=[^:]+):0")


def get_export_base():
    env_path = os.getenv("SVN_CHECK_EXPORT_BASE")
    if env_path:
        return Path(env_path).expanduser()
    return Path("runtime/export")


def get_export_http_root():
    return os.getenv("SVN_CHECK_DOWNLOAD_ROOT", "http://localhost:8500/exports")


def build_export_download_url(path_str):
    export_base = get_export_base()
    path_obj = Path(path_str)
    try:
        rel_path = path_obj.relative_to(export_base)
    except ValueError:
        return ""

    quoted_rel_path = "/".join(quote(part) for part in rel_path.parts)
    return f"{get_export_http_root().rstrip('/')}/{quoted_rel_path}"


def build_export_download_link(path_str, label="下载代码"):
    download_url = build_export_download_url(path_str)
    if not download_url:
        return ""
    return f'<a href="{download_url}" target="_blank">{label}</a>'


def safe_remove_prefix(path_str):
    p = Path(path_str)
    prefix = get_export_base()
    try:
        return str(p.relative_to(prefix))
    except ValueError:
        return path_str


def get_filename(path_str):
    return Path(path_str).name


def normalize_path(path_str):
    if pd.isna(path_str) or not str(path_str).strip():
        return None

    s = str(path_str).strip().replace("\\", "/")

    if len(s) >= 2 and s[1] == ":":
        s = s[2:]

    s = s.lstrip("/")

    while "//" in s:
        s = s.replace("//", "/")

    return s.lower()


def tail_path(path_str, levels=3):
    """
    只取路径最后几层，增强 windows/linux 根路径不一致时的匹配能力
    """
    norm = normalize_path(path_str)
    if not norm:
        return None
    parts = norm.split("/")
    return "/".join(parts[-levels:])


def merge_job_program(job_df, program_df):
    job_join_col = job_df.columns[4]
    prog_join_col = program_df.columns[1]
    merge_df = job_df.merge(
        program_df, left_on=job_join_col, right_on=prog_join_col, how="inner"
    )
    return merge_df


def build_program_lookup(merge_df, prog_path_col, tail_levels=4):
    if prog_path_col is None:
        raise ValueError("未传入程序路径列名 prog_path_col")
    if prog_path_col not in merge_df.columns:
        raise ValueError(f"程序路径列不存在: {prog_path_col}")

    selected_columns = [merge_df.columns[2], merge_df.columns[9], merge_df.columns[27]]
    path_tails = merge_df[prog_path_col].apply(lambda x: tail_path(x, tail_levels))
    lookup = {}
    for path_tail, row in zip(
        path_tails,
        merge_df[selected_columns].itertuples(index=False, name=None),
        strict=False,
    ):
        if not path_tail or path_tail in lookup:
            continue
        lookup[path_tail] = list(row)
    return lookup


def get_program_lookup_result(program_lookup, input_path, tail_levels=4):
    input_tail = tail_path(input_path, tail_levels)
    if input_tail in program_lookup:
        return program_lookup[input_tail]
    raise ValueError(f"未找到匹配程序路径: {input_path}")


def _dependency_items(input_string):
    if input_string is None or pd.isna(input_string):
        return []
    return [part[3:] for part in str(input_string).split("|") if part.startswith("33:")]


def _table_name_from_program_path_value(path_value):
    p = Path(str(path_value))
    folder = p.parent.name
    if "." not in folder:
        return None
    schame, table_name = folder.split(".", 1)
    schame = schame.replace("DWS_", "")
    return f"{schame}.{table_name}"


def build_dependency_table_lookup(merge_df):
    dependency_col = merge_df.columns[2]
    program_path_col = merge_df.columns[-6]
    lookup = {}
    for dependency_item, path_value in merge_df[
        [dependency_col, program_path_col]
    ].itertuples(index=False, name=None):
        if dependency_item in lookup:
            continue
        table_name = _table_name_from_program_path_value(path_value)
        lookup[dependency_item] = table_name if table_name else dependency_item
    return lookup


def get_yilai_table_from_lookup(input_string, dependency_table_lookup):
    matched_values = []
    for item in _dependency_items(input_string):
        table_name = dependency_table_lookup.get(item)
        if table_name:
            matched_values.append(table_name)
    return matched_values


def build_job_outfile_lookup(rows):
    lookup = {}
    for row in rows or []:
        if not row or len(row) < 2:
            continue
        job_name = str(row[0]).strip().upper()
        outfile_value = "" if row[1] is None or pd.isna(row[1]) else str(row[1]).strip()
        if job_name and job_name not in lookup:
            lookup[job_name] = outfile_value
    return lookup


def build_wide_table_lineage_summary(
    merge_df,
    input_path,
    *,
    job_outfile_lookup=None,
    result_table_recv_detail_map=None,
    tail_levels=4,
):
    summary = {
        "plan_name": "",
        "job_name": "",
        "program_name": "",
        "program_path": "",
        "result_table": "",
        "dependency_jobs": [],
        "dependency_result_tables": [],
        "recv_plan": "",
        "source_system": "",
        "outfile": "",
        "missing_steps": [],
        "source_fields": [],
    }

    if merge_df is None or merge_df.empty:
        summary["missing_steps"].append("未匹配到作业/程序元数据")
        return summary

    program_path_col = merge_df.columns[-6]
    input_tail = tail_path(input_path, tail_levels)
    matched_df = merge_df[
        merge_df[program_path_col].apply(lambda x: tail_path(x, tail_levels))
        == input_tail
    ]
    if matched_df.empty:
        summary["missing_steps"].append("未找到对应 program")
        return summary

    row = matched_df.iloc[0]
    job_outfile_lookup = job_outfile_lookup or {}
    result_table_recv_detail_map = result_table_recv_detail_map or {}

    plan_name = "" if pd.isna(row.iloc[0]) else str(row.iloc[0]).strip()
    job_name = "" if pd.isna(row.iloc[2]) else str(row.iloc[2]).strip()
    program_name = "" if pd.isna(row.iloc[4]) else str(row.iloc[4]).strip()
    dependency_raw = "" if pd.isna(row.iloc[27]) else str(row.iloc[27]).strip()
    program_path = "" if pd.isna(row.iloc[32]) else str(row.iloc[32]).strip()
    result_table = _table_name_from_program_path_value(program_path) or ""
    dependency_jobs = _dependency_items(dependency_raw)
    dependency_lookup = build_dependency_table_lookup(merge_df)
    dependency_result_tables = get_yilai_table_from_lookup(
        dependency_raw, dependency_lookup
    )

    recv_plan = ""
    source_system = ""
    if dependency_result_tables:
        for dependency_table in dependency_result_tables:
            details = result_table_recv_detail_map.get(
                str(dependency_table).strip().upper(), []
            )
            if not details:
                continue
            recv_plan = details[0].get("recv_plan", "") or recv_plan
            source_system = details[0].get("source_system", "") or source_system
            if recv_plan or source_system:
                break

    outfile = ""
    for dependency_job in dependency_jobs:
        outfile = job_outfile_lookup.get(str(dependency_job).strip().upper(), "")
        if outfile:
            break

    if not dependency_jobs:
        summary["missing_steps"].append("未解析到依赖作业")
    if dependency_jobs and not dependency_result_tables:
        summary["missing_steps"].append("未匹配依赖结果表")
    if not recv_plan:
        summary["missing_steps"].append("未匹配 recv plan")
    if not source_system:
        summary["missing_steps"].append("未匹配来源系统")
    if dependency_jobs and not outfile:
        summary["missing_steps"].append("未匹配 outfile")

    program_path_display = tail_path(program_path, 4) or Path(program_path).name

    summary.update(
        {
            "plan_name": plan_name,
            "job_name": job_name,
            "program_name": program_name,
            "program_path": program_path_display,
            "result_table": result_table,
            "dependency_jobs": dependency_jobs,
            "dependency_result_tables": dependency_result_tables,
            "recv_plan": recv_plan,
            "source_system": source_system,
            "outfile": outfile,
            "source_fields": [
                {
                    "链路节点": "plan",
                    "值": plan_name or "未匹配",
                    "来源 metadata": "jobs",
                    "来源字段": "a",
                    "状态": "命中" if plan_name else "未匹配",
                },
                {
                    "链路节点": "job",
                    "值": job_name or "未匹配",
                    "来源 metadata": "jobs",
                    "来源字段": "c",
                    "状态": "命中" if job_name else "未匹配",
                },
                {
                    "链路节点": "program",
                    "值": program_name or "未匹配",
                    "来源 metadata": "jobs -> programs",
                    "来源字段": "e -> b",
                    "状态": "命中" if program_name else "未匹配",
                },
                {
                    "链路节点": "result table",
                    "值": result_table or "未匹配",
                    "来源 metadata": "programs",
                    "来源字段": "e(路径推导)",
                    "状态": "命中" if result_table else "未匹配",
                },
                {
                    "链路节点": "dependency job",
                    "值": " | ".join(dependency_jobs) if dependency_jobs else "未匹配",
                    "来源 metadata": "jobs",
                    "来源字段": "ab",
                    "状态": "命中" if dependency_jobs else "未匹配",
                },
                {
                    "链路节点": "dependency result table",
                    "值": " | ".join(dependency_result_tables)
                    if dependency_result_tables
                    else "未匹配",
                    "来源 metadata": "jobs -> programs",
                    "来源字段": "ab -> e -> 路径",
                    "状态": "命中" if dependency_result_tables else "未匹配",
                },
                {
                    "链路节点": "recv plan",
                    "值": recv_plan or "未匹配",
                    "来源 metadata": "result_receipts",
                    "来源字段": "recv_plan",
                    "状态": "命中" if recv_plan else "未匹配",
                },
                {
                    "链路节点": "source system",
                    "值": source_system or "未匹配",
                    "来源 metadata": "receive_plans",
                    "来源字段": "sys_name",
                    "状态": "命中" if source_system else "未匹配",
                },
                {
                    "链路节点": "outfile",
                    "值": outfile or "未匹配",
                    "来源 metadata": "job_outputs",
                    "来源字段": "b",
                    "状态": "命中" if outfile else "未匹配",
                },
            ],
        }
    )
    return summary


def get_result(merge_df, input_path, tail_levels=3, prog_path_col=None):
    input_tail = tail_path(input_path, tail_levels)
    print("================ get_result debug ================")
    print(f"input_path: {input_path}")
    print(f"input_tail: {input_tail}")
    print(f"all columns: {list(merge_df.columns)}")

    print(f"prog_path_col: {prog_path_col}")
    if prog_path_col is None:
        raise ValueError("未传入程序路径列名 prog_path_col")
    if prog_path_col not in merge_df.columns:
        raise ValueError(f"程序路径列不存在: {prog_path_col}")

    df = merge_df.copy()
    df["_path_tail"] = df[prog_path_col].apply(lambda x: tail_path(x, tail_levels))
    print("sample program paths:")
    print(df[[prog_path_col, "_path_tail"]].head(20).to_string())
    result = df[df["_path_tail"] == input_tail].copy()

    if result.empty:
        print("no matched rows found, fuzzy samples:")
        fuzzy_df = df[
            df["_path_tail"]
            .astype(str)
            .str.contains(get_filename(input_path).split(".")[0], case=False, na=False)
        ]
        if fuzzy_df.empty:
            print(df[[prog_path_col, "_path_tail"]].tail(20).to_string())
        else:
            print(fuzzy_df[[prog_path_col, "_path_tail"]].to_string())
        raise ValueError(f"未找到匹配程序路径: {input_path}")

    selected_columns = [result.columns[2], result.columns[9], result.columns[27]]
    result = result[selected_columns]
    return result.iloc[0].tolist()


def match_path(path, pattern):
    return fnmatch.fnmatch(Path(path).as_posix(), pattern)


def match_any(path, patterns):
    path = Path(path).as_posix()
    return any(fnmatch.fnmatch(path, p) for p in patterns)


def load_txt_to_df2(file_path, columns):
    data = []

    with open(file_path, encoding="utf-8") as f:
        for line in f:
            line = line.rstrip("\n").rstrip("\r")
            parts = line.split(",", 1)  # 只按第一个逗号切一次

            if len(parts) == 2:
                data.append(parts)
            else:
                data.append([parts[0], None])  # 没有逗号的情况

    return pd.DataFrame(data, columns=columns)


def load_xls_to_df(file_path):
    """
    读取无表头txt并转换为DataFrame
    参数：
    file_path: 文件路径
    columns: 列名列表
    sep: 分隔符（默认逗号，可改为\t、|等）
    encoding: 文件编码（默认utf-8）
    返回：
    pandas DataFrame
    """
    df = pd.read_excel(file_path, engine="xlrd")
    return df


def load_txt_to_df(file_path, columns, sep=",", encoding="utf-8"):
    """
    读取无表头txt并转换为DataFrame
    参数：
    file_path: 文件路径
    columns: 列名列表
    sep: 分隔符（默认逗号，可改为\t、|等）
    encoding: 文件编码（默认utf-8）
    返回：
    pandas DataFrame
    """
    df = pd.read_csv(
        file_path,
        sep=sep,
        header=None,  # 关键：无表头
        names=columns,  # 指定列名
        encoding=encoding,
    )
    return df


"""判断字符是否是中文"""


def is_chinese(char):
    return "\u4e00" <= char <= "\u9fff"


def clear_folder(folder_path):
    shutil.rmtree(folder_path)
    os.makedirs(folder_path)


"""
读取所有带.的字符串
"""


def find_dot_strings(sql):
    # 使用正则表达式匹配带有"."的字符串
    matches = DOT_STRING_PATTERN.findall(sql)
    reslut = []
    matches = [match for match in matches if not any(is_chinese(c) for c in match)]
    for i in matches:
        i = i.upper()
        if i.split(".")[0] in ("DM", "DWA", "DWD", "DWE", "DWM", "DWP", "DWO", "DWF"):
            reslut.append(i)
    return reslut


"""
读取from和join 后面的表名
"""


def extract_tables(sql_query):
    # 使用正则表达式匹配 FROM 和 LEFT JOIN 后面的表名
    matches = TABLE_PATTERN.findall(sql_query)
    matches = [match for match in matches if not any(is_chinese(c) for c in match)]
    tables = set()
    for match in matches:
        table_name = match[1].strip()
        if table_name:
            tables.add(table_name.upper())
    return list(tables)


def find_hardcoded_dates(sql_content):
    # 定义日期的正则表达式模式
    # 查找所有匹配的日期
    matches = DATE_PATTERN.findall(sql_content)
    # 筛选出有效的日期
    valid_dates = []
    for match in matches:
        match = match.strip("'")
        try:
            if "-" in match:
                date_obj = datetime.strptime(match, "%Y-%m-%d")
            else:
                date_obj = datetime.strptime(match, "%Y%m%d")
            # 检查年份是否在1900到3000之间
            if 1900 <= date_obj.year <= 3000:
                valid_dates.append(match)
        except ValueError:
            # 如果转换失败，则认为这不是一个有效的日期
            continue
    return valid_dates


def find_unique_elements_with_count(a, b):
    counter_A = Counter(a)
    counter_B = Counter(b)
    c = []
    d = []
    for element in counter_A:
        if element not in counter_B:
            c.extend([element] * counter_A[element])
    for element in counter_B:
        if element not in counter_A:
            d.extend([element] * counter_B[element])
    return c, d


def read_excel_file(file_path):
    print("===============read_excel_file================")
    workbook = xlrd.open_workbook(file_path)
    worksheet = workbook.sheet_by_index(0)
    data = [worksheet.row_values(rownum) for rownum in range(1, worksheet.nrows)]
    return data


def extract_values(input_string):
    print("===============extract_values================")
    # 定义正则表达式模式来匹配 outfile= 和 proname= 的值
    # 查找所有匹配项
    outfile_matches = OUTFILE_PATTERN.findall(input_string)
    # 提取值并去除前缀
    outfile_value = [match.replace("outfile=", "") for match in outfile_matches]
    if len(outfile_value) > 0:
        outfile_value = outfile_value[0]
    else:
        outfile_value = ""
    return outfile_value


def detect_file_format(file_path):
    with open(file_path, "rb") as file:
        first_line = file.readline()
        if b"\r\n" in first_line:
            return "DOS"
        elif b"\n" in first_line:
            return "Unix"
        else:
            return "Unknown"


def read_data_from_file(file_path):
    print(
        "===================================read_data_from_file================================="
    )
    if os.path.exists(file_path):
        with open(file_path, "rb") as file:
            raw_content = file.read()

        for encoding in ("utf-8", "utf-8-sig", "gb18030", "gbk"):
            try:
                return raw_content.decode(encoding)
            except UnicodeDecodeError:
                continue

        return raw_content.decode("utf-8", errors="replace")
    else:
        print(file_path + " 不存在")
        return ""


def ifmiaoshu(miaoshu, job_name):
    pattern = re.compile(r"^[A-Za-z0-9]+$")
    if miaoshu == "":
        return True
    if miaoshu is None:
        return True
    if miaoshu == "数据供应作业":
        return True
    ms = (
        miaoshu.replace("加工表[", "")
        .replace("接入表[", "")
        .replace("模型层[", "")
        .replace("新国结表[", "")
        .replace("]数据采集作业", "")
        .replace("]数据加工作业", "")
        .replace("]数据预处理作业", "")
        .replace("]数据装载作业", "")
        .replace("数据供应作业SEND:", "")
        .replace("数据采集作业", "")
        .replace("数据装载加工作业", "")
        .replace("-数据供应作业", "")
        .replace("合规示例-", "")
        .replace("合规示例-LDM-JGJS_", "")
        .replace("卸数", "")
        .replace("数据供应", "")
        .replace("全量", "")
        .replace("增量", "")
        .replace("实时数据供应", "")
        .replace("拉链表", "")
        .replace("拉链", "")
        .replace("数据", "")
        .replace("F层", "")
        .replace("贴源层", "")
        .replace("装载", "")
        .replace("采集", "")
        .replace("预处理", "")
        .replace("[", "")
        .replace("]", "")
        .replace("作业", "")
        .replace("加工", "")
        .replace("-", "")
        .replace("国结模型表", "")
        .replace("新国结表", "")
        .replace("新国结", "")
        .replace("国结表", "")
        .replace("国结", "")
    )
    if ms == "":
        return True
    if len(ms) <= 1:
        return True
    if ms in job_name:
        return True
    return bool(pattern.match(ms))
