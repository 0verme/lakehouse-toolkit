"""#35 Golden Corpus 的公开 synthetic program inventory。

程序内容复用 Phase 4 的完全虚构 SQL fixture；这里明确标出 corpus 用途，避免
把真实内网程序、SQL 或表名带入公开 corpus。
"""

from tests.fixtures.lineage.phase4_audit_programs import (
    CYCLE_PROGRAM,
    MULTI_SINK_PROGRAM,
    NORMAL_PROGRAM,
    ORPHAN_BRANCH_PROGRAM,
    SELF_REFERENCE_PROGRAM,
    TARGET_MISMATCH_PROGRAM,
    TARGET_NOT_FOUND_PROGRAM,
)

SYNTHETIC_PROGRAMS = {
    "normal_negative_control": NORMAL_PROGRAM,
    "orphan_branch": ORPHAN_BRANCH_PROGRAM,
    "multi_sink_candidate": MULTI_SINK_PROGRAM,
    "target_not_found": TARGET_NOT_FOUND_PROGRAM,
    "target_mismatch": TARGET_MISMATCH_PROGRAM,
    "self_reference": SELF_REFERENCE_PROGRAM,
    "cycle_detected": CYCLE_PROGRAM,
}

__all__ = ["SYNTHETIC_PROGRAMS"]
