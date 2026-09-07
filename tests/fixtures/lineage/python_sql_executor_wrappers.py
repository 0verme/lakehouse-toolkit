def get_runtime_date():
    return "runtime-only"


class Executor:
    def do(self, sql):
        pass


executor = Executor()


def main():
    runtime_date = get_runtime_date()

    sql1 = """
    delete from DWD.D_GJFK_ALGJ
    where dw_data_dt = to_date('{DATE}', 'YYYYMMDD')
    """.format(DATE=runtime_date)

    sql2 = """
    insert into DWD.D_GJFK_ALGJ (...)
    select ...
    from ODS.SOURCE_A
    where dt = '{DATE}'
    """.format(DATE=runtime_date)

    executor.do(sql1)
    executor.do(sql2)

    return 0
