def run_program(execute):
    sql_step_1 = """
    CREATE TEMPORARY TABLE TMP_1 AS
    SELECT a.id, b.amount
    FROM ODS.DEMO_A a
    JOIN DWF.DEMO_B b ON a.id = b.id;
    """
    execute(sql_step_1)

    sql_step_2 = """
    CREATE TEMP TABLE TMP_2 AS
    SELECT t.id, c.category
    FROM TMP_1 t
    JOIN DWM.DEMO_C c ON t.id = c.id;
    """
    execute(sql_step_2)

    sql_step_3 = """
    INSERT OVERWRITE TABLE DWA.DEMO_RESULT
    SELECT t.id, d.flag
    FROM TMP_2 t
    JOIN DWA.DEMO_D d ON t.id = d.id;
    """
    execute(sql_step_3)
