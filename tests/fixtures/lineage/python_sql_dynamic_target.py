def load_daily(self, source_columns):
    dynamic_insert = """
    INSERT INTO DLO.{0} PARTITION(DW_DATA_DT = '{1}')
    SELECT {2}
    FROM DLO.DEMO_SOURCE_REAL
    WHERE DW_DATA_DT = '{1}'
    """.format(
        self.runtime_table_name,
        self.batch_date,
        ",".join(source_columns),
    )
    execute(dynamic_insert)

    execute("""
    INSERT INTO DWO.DEMO_STAGE
    SELECT * FROM DLO.DEMO_SOURCE_REAL
    """)

    execute("""
    INSERT INTO DWM.DEMO_RESULT
    SELECT * FROM DWO.DEMO_STAGE
    JOIN DWF.DEMO_BUSINESS_SOURCE ON 1 = 1
    """)
