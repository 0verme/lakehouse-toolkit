SQL_TEXT = """
CREATE TABLE DWM.M_DEMO_ACCT (
    cust_id text comment 'demo customer id',
    acct_id text comment 'demo account id',
    biz_date text comment 'demo biz date',
    amount numeric(18,2) comment 'demo amount',
    demo_flag text comment 'demo flag'
);

INSERT INTO DWM.M_DEMO_ACCT
SELECT
    c.cust_id,
    c.acct_id,
    c.biz_date,
    DEMO_AMOUNT_BUCKET(c.amount) AS amount,
    c.demo_flag
FROM dwp.v_demo_customer c
LEFT JOIN DWS.PARA_DEMO_RATE p
    ON 1 = 1
LEFT JOIN DWP.DWE_DEMO_PUSH_RESULT r
    ON c.cust_id = r.cust_id;
"""


def run():
    return SQL_TEXT
