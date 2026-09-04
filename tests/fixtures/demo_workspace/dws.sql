create table DWM.M_DEMO_ACCT (
    cust_id text comment 'demo customer id',
    acct_id text comment 'demo account id',
    biz_date text comment 'demo biz date',
    amount numeric(18,2) comment 'demo amount',
    demo_flag text comment 'demo flag'
);

insert into DWM.M_DEMO_ACCT
select
    cust_id,
    acct_id,
    biz_date,
    DEMO_AMOUNT_BUCKET(amount) as amount,
    demo_flag
from dwp.v_demo_customer;
