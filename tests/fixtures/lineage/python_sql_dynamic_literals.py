"""脱敏的生产形态 fixture：静态 SQL 模板 + 动态 literal。

覆盖两类已确认的 PROD 结构：

- ``str.replace(...).format(...)``：replace 与 format 只改写 literal，
  schema / table identifier 保持静态。
- f-string：动态变量只出现在日期等 literal 中。

所有表名、字段名与变量均为虚构，不含真实生产资产。
"""


def build_replace_format_sql(executor, batchflg, runtime_vars):
    """REPLACE + FORMAT 只替换 literal，table lineage 应保持可静态恢复。"""

    sqlstr = """
    CREATE TABLE DWS_DWUPRR.TMP_DEMO_02 AS
    SELECT *
    FROM DWS_DWUPRR.DEMO_EXPOSURE
    WHERE datadate = '{DATE}'
      AND batch_flag = 'batchflg'
    """.replace("batchflg", batchflg).format(**runtime_vars)
    executor.do(sqlstr)


def build_f_string_sql(executor, run_date):
    """f-string 的动态值只出现在日期 literal，source/target 完全静态。"""

    sqlstr = f"""
    MERGE INTO DWS_DWUPRR.DEMO_TARGET t
    USING (
        SELECT *
        FROM DWS_DWF.DEMO_SOURCE
        WHERE start_dt <= date'{run_date}'
    ) s
    ON t.id = s.id
    """
    executor.do(sqlstr)