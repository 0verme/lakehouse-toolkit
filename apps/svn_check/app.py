import sys
from importlib import import_module
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[2]
APP_DIR = Path(__file__).resolve().parent

for path in (ROOT_DIR, APP_DIR):
    path_str = str(path)
    if path_str not in sys.path:
        sys.path.insert(0, path_str)

from services.diag_service import (  # noqa: E402
    begin_app_rerun,
    end_app_rerun,
    log_button_event,
    log_exception_event,
    log_session_state_snapshot,
    log_task_event,
    log_widget_changes,
    log_widget_snapshot,
    record_idle_rerun_signal,
    run_static_risk_scan,
)
from services.workspace_service import load_local_workspace  # noqa: E402
from ui.fine_stream import fine_stream  # noqa: E402
from ui.lakehouse_stream import lakehouse_stream  # noqa: E402
from ui.upstream_stream import upstream_stream  # noqa: E402

st = import_module("streamlit")

st.set_page_config(
    page_title="代码审查平台",
    page_icon=None,
    layout="wide",
)


def detect_project(
    source_value: str,
    strict_mode: bool,
    debug_mode: bool,
    source_mode: str = "svn",
    local_project: str = "",
    workspace_info=None,
):
    log_task_event(
        "detect_project",
        "start",
        branch_url=source_value,
        source_mode=source_mode,
        local_project=local_project,
        strict_mode=bool(strict_mode),
        debug_mode=bool(debug_mode),
    )
    if source_mode == "local":
        if local_project == "lakehouse":
            lakehouse_stream(
                strict_mode,
                status_box,
                log_box,
                progress_bar,
                step_text,
                "lakehouse",
                source_value,
                debug_mode=debug_mode,
                workspace_info=workspace_info,
            )
        else:
            st.warning(f"本地目录模式暂不支持: {local_project}")
            st.stop()
    elif "/lakehouse/" in source_value:
        lakehouse_stream(
            strict_mode,
            status_box,
            log_box,
            progress_bar,
            step_text,
            "lakehouse",
            source_value,
            debug_mode=debug_mode,
        )
    elif "/UPSTREAM/" in source_value or "/upstream/" in source_value:
        upstream_stream(
            strict_mode,
            status_box,
            log_box,
            progress_bar,
            step_text,
            "upstream",
            source_value,
        )
    elif "/reporting/" in source_value:
        fine_stream(
            strict_mode,
            status_box,
            log_box,
            progress_bar,
            step_text,
            "reporting",
            source_value,
        )
    else:
        st.warning(f"输入链接暂不支持: {source_value}")
        st.stop()
    log_task_event(
        "detect_project",
        "end",
        branch_url=source_value,
        source_mode=source_mode,
        local_project=local_project,
    )


render_completed = False
begin_app_rerun()
run_static_risk_scan()

try:
    st.title("数据开发代码审查")
    st.caption("适用于本地数据开发、报表和调度文件的自动 review")

    source_mode = st.radio(
        "运行模式",
        ["SVN 分支地址", "本地目录审计"],
        horizontal=True,
        key="source_mode_input",
    )

    with st.form("svn_check_form", clear_on_submit=False):
        svn_path = ""
        local_dir = ""
        local_project = "LAKEHOUSE"
        if source_mode == "SVN 分支地址":
            svn_path = st.text_input(
                "SVN 分支地址",
                placeholder="例如: svn://svn.example.invalid/lakehouse、reporting、upstream 分支地址",
                key="svn_path_input",
            )
        else:
            local_dir = st.text_input(
                "本地目录路径",
                placeholder="例如: ./examples/workspace",
                key="local_dir_input",
            )
            local_project = st.selectbox(
                "模块类型",
                ["LAKEHOUSE"],
                key="local_project_input",
            )

        col1, col2, col3 = st.columns([2, 1, 1])
        with col1:
            strict_mode = st.checkbox(
                "接入本地 AI 大模型", value=False, key="strict_mode_input"
            )
        with col2:
            debug_mode = st.checkbox("调试日志", value=False, key="debug_mode_input")
        with col3:
            submitted = st.form_submit_button(
                "开始检查", use_container_width=True, type="primary"
            )

    widget_values = {
        "source_mode_input": source_mode,
        "svn_path_input": svn_path,
        "local_dir_input": local_dir,
        "local_project_input": local_project,
        "strict_mode_input": strict_mode,
        "debug_mode_input": debug_mode,
        "form_submitted": submitted,
    }
    log_widget_snapshot(widget_values, "POST_FORM_RENDER")
    widget_changes = log_widget_changes(widget_values, "POST_FORM_RENDER")
    log_task_event(
        "form_state", "observed", submitted=bool(submitted), source_mode=source_mode
    )
    record_idle_rerun_signal(
        submitted=bool(submitted),
        widget_changed=bool(widget_changes),
    )

    status_box = st.empty()
    progress_bar = st.empty()
    step_text = st.empty()
    log_box = st.empty()

    if submitted:
        log_task_event(
            "form_submit", "entered", submitted=True, source_mode=source_mode
        )
        log_button_event("svn_check_form_submit", "clicked")
        log_session_state_snapshot("BUTTON_BEFORE_VALIDATE")

        if source_mode == "SVN 分支地址":
            svn_path = (svn_path or "").strip()
            if not svn_path:
                log_task_event(
                    "form_submit",
                    "validation_failed",
                    reason="empty_svn_path",
                    source_mode="svn",
                )
                log_button_event(
                    "svn_check_form_submit",
                    "validation_failed",
                    reason="empty_svn_path",
                )
                log_session_state_snapshot("BUTTON_AFTER_VALIDATE_FAIL")
                st.warning("SVN / GIT 路径不能为空")
                st.stop()

            log_task_event(
                "form_submit",
                "validation_passed",
                svn_path_length=len(svn_path),
                source_mode="svn",
            )
            log_button_event(
                "svn_check_form_submit",
                "validated",
                svn_path_length=len(svn_path),
                source_mode="svn",
                strict_mode=bool(strict_mode),
                debug_mode=bool(debug_mode),
            )
            log_session_state_snapshot("BUTTON_BEFORE_TASK")
            try:
                detect_project(svn_path, strict_mode, debug_mode, source_mode="svn")
            except Exception as exc:
                log_exception_event("BUTTON_TASK_EXCEPTION", exc, svn_path=svn_path)
                log_session_state_snapshot("BUTTON_TASK_EXCEPTION")
                raise

            log_button_event(
                "svn_check_form_submit",
                "task_completed",
                svn_path_length=len(svn_path),
                source_mode="svn",
            )
        else:
            local_dir = (local_dir or "").strip()
            local_project_value = local_project.lower()
            if not local_dir:
                log_task_event(
                    "form_submit",
                    "validation_failed",
                    reason="empty_local_dir",
                    source_mode="local",
                )
                log_button_event(
                    "svn_check_form_submit",
                    "validation_failed",
                    reason="empty_local_dir",
                )
                log_session_state_snapshot("BUTTON_AFTER_VALIDATE_FAIL")
                st.warning("请输入本地目录路径")
                st.stop()

            local_path = Path(local_dir).expanduser()
            if not local_path.exists():
                log_task_event(
                    "form_submit",
                    "validation_failed",
                    reason="missing_local_dir",
                    source_mode="local",
                    local_dir=local_dir,
                )
                log_button_event(
                    "svn_check_form_submit",
                    "validation_failed",
                    reason="missing_local_dir",
                )
                log_session_state_snapshot("BUTTON_AFTER_VALIDATE_FAIL")
                st.error("本地目录不存在，请检查路径")
                st.stop()
            if not local_path.is_dir():
                log_task_event(
                    "form_submit",
                    "validation_failed",
                    reason="invalid_local_dir",
                    source_mode="local",
                    local_dir=local_dir,
                )
                log_button_event(
                    "svn_check_form_submit",
                    "validation_failed",
                    reason="invalid_local_dir",
                )
                log_session_state_snapshot("BUTTON_AFTER_VALIDATE_FAIL")
                st.error("本地目录不是有效目录，请检查路径")
                st.stop()

            log_task_event(
                "form_submit",
                "validation_passed",
                local_dir_length=len(local_dir),
                source_mode="local",
                local_project=local_project_value,
            )
            log_button_event(
                "svn_check_form_submit",
                "validated",
                local_dir_length=len(local_dir),
                source_mode="local",
                local_project=local_project_value,
                strict_mode=bool(strict_mode),
                debug_mode=bool(debug_mode),
            )
            log_session_state_snapshot("BUTTON_BEFORE_TASK")
            try:
                workspace_info = load_local_workspace(local_dir)
                detect_project(
                    local_dir,
                    strict_mode,
                    debug_mode,
                    source_mode="local",
                    local_project=local_project_value,
                    workspace_info=workspace_info,
                )
            except (FileNotFoundError, NotADirectoryError) as exc:
                log_exception_event("BUTTON_TASK_EXCEPTION", exc, local_dir=local_dir)
                log_session_state_snapshot("BUTTON_TASK_EXCEPTION")
                st.error("本地目录不存在，请检查路径")
                st.stop()
            except ValueError as exc:
                log_exception_event("BUTTON_TASK_EXCEPTION", exc, local_dir=local_dir)
                log_session_state_snapshot("BUTTON_TASK_EXCEPTION")
                st.error(str(exc))
                st.stop()
            except Exception as exc:
                log_exception_event("BUTTON_TASK_EXCEPTION", exc, local_dir=local_dir)
                log_session_state_snapshot("BUTTON_TASK_EXCEPTION")
                raise

            log_button_event(
                "svn_check_form_submit",
                "task_completed",
                local_dir_length=len(local_dir),
                source_mode="local",
                local_project=local_project_value,
            )
        log_session_state_snapshot("BUTTON_AFTER_TASK")

    render_completed = True
except Exception as exc:
    log_exception_event("APP_RERUN_EXCEPTION", exc)
    raise
finally:
    end_app_rerun(render_completed=render_completed)
