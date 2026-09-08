sql = """
-- legacy comment contains \N
insert into DEMO_DWM.RESULT_A
select *
from DEMO_DWF.SOURCE_A
"""
executor.do(sql)
