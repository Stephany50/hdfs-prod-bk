INSERT INTO tmp.TMP_CUST_CONTACT_ORDER
SELECT a.customer_id, custcode, prgcode, ccname, ccline2, ccline3,  cast(ohstatus as string) ohstatus, ohentdate, ohrefnum, balance, billcycle,
    (CASE WHEN b.customer_id IS NOT NULL /*AND ohstatus != 'DP'*/ THEN balance ELSE 0 END) AS balance_prev_period,
    (CASE WHEN b.customer_id IS NOT NULL AND cast(ohstatus as string) != 'DP' THEN balance ELSE 0 END) AS balance_current,
    (CASE WHEN /* b.customer_id IS NOT NULL AND ohstatus = 'IN' AND*/ ohrefnum IS NOT NULL THEN ohentdate ELSE NULL END) AS bill_date
    , '2019-12-01' event_date, current_timestamp insert_date -- ajout par snr
FROM
(
        select custid customer_id, guid, AcctGuid, custseg prgcode, BILL_CYCLE_ID BILLCYCLE, total_bill_amount/100 cscurbalance, custname ccname, firstname ccline2, lastname ccline3
        from cdr.IT_CUST_FULL
        where original_file_date = '2019-12-01' --and AcctGuid is null
            and nvl(total_bill_amount, 0) != 0 --and bill_cycle_id in ('to_define')
) a
LEFT JOIN
(
    select cust_id customer_id, b.account_number custcode, null ohstatus, b.bill_date ohentdate, b.invoice_number ohrefnum, b.remaining_amount/100 as balance
    from cdr.IT_BILL b
    where original_file_date = '2019-12-01'
) b
    ON a.customer_id = b.customer_id

create table tmp.TMP_CUST_CONTACT_ORDER_2 as

insert into tmp.TMP_CUST_CONTACT_ORDER_2
SELECT a.customer_id, custcode, prgcode, ccname, ccline2, ccline3,  cast(ohstatus as string) ohstatus, ohentdate, ohrefnum, balance, billcycle,
    (CASE WHEN b.customer_id IS NOT NULL /*AND ohstatus != 'DP'*/ THEN balance ELSE 0 END) AS balance_prev_period,
    balance balance_current,
    (CASE WHEN /* b.customer_id IS NOT NULL AND ohstatus = 'IN' AND*/ ohrefnum IS NOT NULL THEN ohentdate ELSE NULL END) AS bill_date
    , '2019-12-01' event_date, current_timestamp insert_date -- ajout par snr
FROM
(
        select custid customer_id, guid, AcctGuid, custseg prgcode, max(BILL_CYCLE_ID) BILLCYCLE, sum(total_bill_amount/100) cscurbalance, custname ccname, firstname ccline2, lastname ccline3
        from cdr.IT_CUST_FULL
        where original_file_date = '2019-12-01' --and AcctGuid is null
            and nvl(total_bill_amount, 0) != 0 --and bill_cycle_id in ('to_define')
        group by custid, guid, AcctGuid, custseg, custname, firstname, lastname
) a
LEFT JOIN
(
    select cust_id customer_id, b.account_number custcode, null ohstatus, b.bill_date ohentdate, b.invoice_number ohrefnum, b.remaining_amount/100 as balance
    from cdr.IT_BILL b
    where original_file_date = '2019-12-01'
) b
    ON a.customer_id = b.customer_id

create table tmp.TMP_CUST_CONTACT_ORDER_test as
select a.custid customer_id, a.guid, a.AcctGuid, a.custseg prgcode, a.BILL_CYCLE_ID BILLCYCLE, a.total_bill_amount/100 cscurbalance, a.custname ccname, a.firstname ccline2, a.lastname ccline3
    , b.cust_id customer_id, b.account_number custcode, cast(null as string) ohstatus, b.bill_date ohentdate, b.invoice_number ohrefnum, b.remaining_amount/100 as balance
from cdr.spark_IT_CUST_FULL a left join cdr.spark_IT_BILL b on b.original_file_date = '2019-12-01' and a.custid = b.cust_id
where a.original_file_date = '2019-12-01' --and AcctGuid is null
    and nvl(a.total_bill_amount, 0) != 0

insert into cdr.spark_it_bill select * from cdr.it_bill where original_file_date >= '2019-12-01' ;

insert into cdr.spark_IT_CUST_FULL select * from cdr.IT_CUST_FULL where original_file_date >= '2019-12-01' ;

select original_file_date, count(*), count(distinct custid), count(distinct guid), count(distinct AcctGuid)
from cdr.spark_IT_CUST_FULL where original_file_date >= '2019-12-01' group by original_file_date;

select original_file_date, count(*), count(distinct bill_id), count(distinct cust_id), count(distinct account_number), count(distinct account_number)
from cdr.IT_bill group by original_file_date;

select * from cdr.IT_BILL where original_file_date = '2019-12-18' and account_number='1.10384654'

select * from cdr.IT_cust_full where original_file_date = '2019-12-18' and custid='156056961'