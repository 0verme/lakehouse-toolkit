from importlib import import_module
from pathlib import Path

from services.re_service import match_any, match_path

from core.public_data import all_job, all_program

pd = import_module("pandas")


def is_dws_py(py_url):
    return match_any(
        py_url,
        [
            "**/WORKSPACE/DWM/*.py",
            "**/WORKSPACE/DWA/*.py",
            "**/WORKSPACE/DM/*.py",
            "**/WORKSPACE/DWP/*.py",
            "**/WORKSPACE/DWE/*.py",
            "**/WORKSPACE/DWD/*.py",
        ],
    )


def get_lakehouse_type(r_lists):
    dws_url = ""
    hive_url = ""
    schame_config_lists = []
    sbin_lists = []
    recv_lists = []
    dwo_lists = []
    dwf_lists = []
    dlo_meta_json_lists = []
    dlo_lists = []
    py_lists = []
    plan_xls = ""
    seq_xls = ""
    job_xls = ""
    program_xls = ""
    cale_xls = ""
    for i in r_lists:
        if "dws.sql" in i:
            dws_url = i
        elif "hive.sql" in i:
            hive_url = i
        elif match_path(i, "**/data/sbin/DEMO/**"):
            sbin_lists.append(i)
        elif match_path(i, "**/WORKSPACE/SCHEMA_CONFIG/**"):
            schame_config_lists.append(i)
        elif match_path(
            i, "**/WORKSPACE/DATA_PROJECT/1.0/*DATA_PROJECT.1.0.config.json"
        ):
            recv_lists.append(i)
        elif match_path(i, "**/WORKSPACE/META_DATA/DATA_LANDING/DATA_LANDING.DLO_*"):
            dlo_meta_json_lists.append(i)
        elif match_path(
            i, "**WORKSPACE/DATA_PROJECT/1.0/DATA_LANDING/DATA_LANDING.DLO_"
        ):
            dlo_lists.append(i)
        elif match_path(i, "**WORKSPACE/DATA_PROJECT/1.0/DWS_DWO/DWS_DWO.*.py"):
            dwo_lists.append(i)
        elif match_path(i, "**WORKSPACE/DATA_PROJECT/1.0/DWS_DWF/DWS_DWF.*.py"):
            dwf_lists.append(i)
        elif is_dws_py(i):
            py_lists.append(i)
        elif match_path(i, "*JOB_*.xls"):
            job_xls = i
        elif match_path(i, "*PLAN_*.xls"):
            plan_xls = i
        elif match_path(i, "*SEQ_*.xls"):
            seq_xls = i
        elif match_path(i, "*PROGRAM_*.xls"):
            program_xls = i
        elif match_path(i, "*CALE_*.xls"):
            cale_xls = i

    # 回补扫描：有些 Excel 虽然已导出到同目录，但未出现在本次 diff 列表里。
    fallback_dirs = []
    for xls_path in (job_xls, plan_xls, seq_xls, program_xls, cale_xls):
        if xls_path:
            fallback_dirs.append(str(Path(xls_path).parent))

    for folder in dict.fromkeys(fallback_dirs):
        folder_path = Path(folder)
        if not folder_path.exists():
            continue

        if not job_xls:
            matches = sorted(folder_path.glob("JOB_*.xls"))
            if matches:
                job_xls = str(matches[0])

        if not plan_xls:
            matches = sorted(folder_path.glob("PLAN_*.xls"))
            if matches:
                plan_xls = str(matches[0])

        if not seq_xls:
            matches = sorted(folder_path.glob("SEQ_*.xls"))
            if matches:
                seq_xls = str(matches[0])

        if not program_xls:
            matches = sorted(folder_path.glob("PROGRAM_*.xls"))
            if matches:
                program_xls = str(matches[0])

        if not cale_xls:
            matches = sorted(folder_path.glob("CALE_*.xls"))
            if matches:
                cale_xls = str(matches[0])

    return (
        dws_url,
        hive_url,
        schame_config_lists,
        sbin_lists,
        recv_lists,
        dwo_lists,
        dwf_lists,
        dlo_meta_json_lists,
        dlo_lists,
        py_lists,
        plan_xls,
        seq_xls,
        job_xls,
        program_xls,
        cale_xls,
    )


def all_job_df(job_df, db_data=None):
    if db_data is None:
        db_data = all_job()
    db_df = pd.DataFrame(db_data, columns=job_df.columns)
    merge_df = pd.concat([job_df, db_df], ignore_index=True)
    return merge_df


def all_program_df(program_df):
    db_data = all_program()
    db_df = pd.DataFrame(db_data, columns=program_df.columns)
    merge_df = pd.concat([program_df, db_df], ignore_index=True)
    return merge_df


def get_yilai(input_string):
    print(
        "===================================get_yilai================================="
    )
    parts = input_string.split("|")
    filtered_parts = [part[3:] for part in parts if part.startswith("33:")]
    return filtered_parts


def get_yilai_table(input_string, merge_df):
    r = get_yilai(input_string)
    matched_values = []
    for item in r:
        matched_rows = merge_df[merge_df.iloc[:, 2] == item]
        if not matched_rows.empty:
            third_last_column_value = matched_rows.iloc[:, -6].values[0]
            p = Path(third_last_column_value)
            folder = p.parent.name
            if "." not in folder:
                print(
                    "================ get_yilai_table public program debug ================",
                    flush=True,
                )
                print(f"dependency_item: {item}", flush=True)
                print(f"matched_rows_count: {len(matched_rows)}", flush=True)
                print(f"program_path_value: {third_last_column_value}", flush=True)
                print(f"parent_folder: {folder}", flush=True)
                matched_values.append(item)
                continue
            schame, table_name = folder.split(".", 1)
            schame = schame.replace("DWS_", "")
            matched_values.append(f"{schame}.{table_name}")
    return matched_values
