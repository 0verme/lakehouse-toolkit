sql = """
-- normal AST-success fixture
insert into DEMO_DWM.RESULT_A
select *
from DEMO_DWF.SOURCE_A
"""
executor.do(sql)
