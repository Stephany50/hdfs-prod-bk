select original_file_date, insert_date, count(*)
from cdr.spark_IT_CUST_FULL
where original_file_date >= '###SLICE_VALUE###'
group by original_file_date, insert_date order by original_file_date

select original_file_date, insert_date, count(*)
from cdr.spark_IT_BILL
where original_file_date = '2020-04-05'
group by original_file_date, insert_date order by original_file_date


insert into TMP.TMP_CUST_CONTACT_ORDER_2

create table TMP.TMP_CUST_CONTACT_ORDER3 as
SELECT a.customer_id, custcode, prgcode, ccname, ccline2, ccline3,  cast(ohstatus as string) ohstatus, ohentdate, ohrefnum, balance, billcycle,
    (CASE WHEN b.customer_id IS NOT NULL THEN balance ELSE 0 END) AS balance_prev_period,
    balance balance_current, cscurbalance, sum(last_total_amount) last_total_amount, max(last_total_amount) max_last_total_amount,
    (CASE WHEN ohrefnum IS NOT NULL THEN ohentdate ELSE NULL END) AS bill_date
    , a.original_file_date event_date, current_timestamp insert_date
FROM (
        select original_file_date, custid customer_id, guid, custseg prgcode, max(BILL_CYCLE_ID) BILLCYCLE, sum(total_bill_amount/100) cscurbalance,
            sum(last_total_amount) last_total_amount, custname ccname, firstname ccline2, lastname ccline3
        from (select *--, first_value(total_bill_amount) over (partition by original_file_date, custid, guid order by BILL_CYCLE_ID desc ) last_total_amount
        from CDR.SPARK_IT_CUST_FULL
        where original_file_date between '###SLICE_VALUE###' and '2020-04-05' --and AcctGuid is null
            and nvl(total_bill_amount, 0) != 0 --and bill_cycle_id in ('to_define')
            and custid='155775428' order by BILL_CYCLE_ID limit 5
        ) a group by original_file_date, custid, guid, custseg, custname, firstname, lastname limit 5;
) a LEFT JOIN (
    select distinct original_file_date, cust_id customer_id, b.account_number custcode, null ohstatus, b.bill_date ohentdate, b.invoice_number ohrefnum, b.remaining_amount/100 as balance
    from cdr.spark_IT_BILL b
    where original_file_date between '###SLICE_VALUE###' and '2020-04-05'
) b  ON a.customer_id = b.customer_id and a.original_file_date = b.original_file_date ;

-------------------
insert into TMP.TMP_CUST_CONTACT_ORDER_2
SELECT a.customer_id, custcode, prgcode, ccname, ccline2, ccline3,  cast(ohstatus as string) ohstatus, ohentdate, ohrefnum, balance, billcycle,
    (CASE WHEN b.customer_id IS NOT NULL THEN balance ELSE 0 END) AS balance_prev_period,
    balance balance_current,-- cscurbalance, sum(last_total_amount) last_total_amount, max(last_total_amount) max_last_total_amount,
    (CASE WHEN ohrefnum IS NOT NULL THEN ohentdate ELSE NULL END) AS bill_date
    , a.original_file_date event_date, current_timestamp insert_date
FROM (
        select original_file_date, custid customer_id, guid, custseg prgcode, max(BILL_CYCLE_ID) BILLCYCLE, sum(total_bill_amount/100) cscurbalance, --sum(last_total_amount) last_total_amount,
            custname ccname, firstname ccline2, lastname ccline3
        from CDR.SPARK_IT_CUST_FULL
        where original_file_date between '###SLICE_VALUE###' and '2020-04-07' --and AcctGuid is null
            and nvl(total_bill_amount, 0) != 0 --and bill_cycle_id in ('to_define')
        group by original_file_date, custid, guid, custseg, custname, firstname, lastname
) a LEFT JOIN (
    select distinct original_file_date, cust_id customer_id, b.account_number custcode, null ohstatus, b.bill_date ohentdate, b.invoice_number ohrefnum, b.remaining_amount/100 as balance
    from cdr.spark_IT_BILL b
    where original_file_date between '###SLICE_VALUE###' and '2020-04-07'
) b  ON a.customer_id = b.customer_id and a.original_file_date = b.original_file_date ;


create table  TMP.TT_TMP_BALANCE_AGEE3 as

INSERT INTO TMP.TT_TMP_BALANCE_AGEE_2
SELECT CUSTCODE AS CODE_CLIENT, PRGCODE AS CATEGORIE,
        IF(TRIM(ccname)='', IF(TRIM(ccline2)='', TRIM(ccline3), TRIM(ccline2)), TRIM(ccname)) as nom,
        SUM(nvl(balance_current,0)) AS balance, MAX(bill_date) AS derniere_facture,
        SUM(CASE WHEN ohentdate >= ADD_MONTHS (EVENT_DATE, 0) THEN balance_prev_period ELSE 0 END) AS balance_J,
        SUM(CASE WHEN ohentdate >= ADD_MONTHS (EVENT_DATE, -1) AND ohentdate < ADD_MONTHS (EVENT_DATE, 0) THEN balance_prev_period ELSE 0 END) AS balance_J_30,
        SUM(CASE WHEN ohentdate >= ADD_MONTHS (EVENT_DATE, -2) AND ohentdate < ADD_MONTHS (EVENT_DATE, -1) THEN balance_prev_period ELSE 0 END) AS balance_J_60,
        SUM(CASE WHEN ohentdate >= ADD_MONTHS (EVENT_DATE, -3) AND ohentdate < ADD_MONTHS (EVENT_DATE, -2) THEN balance_prev_period ELSE 0 END) AS balance_J_90,
        SUM(CASE WHEN ohentdate >= ADD_MONTHS (EVENT_DATE, -4) AND ohentdate < ADD_MONTHS (EVENT_DATE, -3) THEN balance_prev_period ELSE 0 END) AS balance_J_120,
        SUM(CASE WHEN ohentdate >= ADD_MONTHS (EVENT_DATE, -5) AND ohentdate < ADD_MONTHS (EVENT_DATE, -4) THEN balance_prev_period ELSE 0 END) AS balance_J_150,
        SUM(CASE WHEN ohentdate >= ADD_MONTHS (EVENT_DATE, -6) AND ohentdate < ADD_MONTHS (EVENT_DATE, -5) THEN balance_prev_period ELSE 0 END) AS balance_J_180,
        SUM(CASE WHEN ohentdate >= ADD_MONTHS (EVENT_DATE, -12) AND ohentdate < ADD_MONTHS (EVENT_DATE, -6) THEN balance_prev_period ELSE 0 END) AS balance_J_360,
        SUM(CASE WHEN ohentdate >= ADD_MONTHS (EVENT_DATE, -24) AND ohentdate < ADD_MONTHS (EVENT_DATE, -12) THEN balance_prev_period ELSE 0 END) AS balance_J_720,
        SUM(CASE WHEN ohentdate < ADD_MONTHS (EVENT_DATE, -24) THEN balance_prev_period ELSE 0 END) AS balance_J_720_Plus,
        billcycle AS BILLCYCLE_CODE, a.customer_id, a.event_date AS DATE_PERIODE_REF, current_timestamp AS INSERT_DATE , a.event_date event_date -- ajout snr
FROM TMP.TMP_CUST_CONTACT_ORDER_2 A
WHERE EVENT_DATE between '###SLICE_VALUE###' and '2020-04-07'
GROUP BY EVENT_DATE, CUSTCODE, PRGCODE, IF(TRIM(CCNAME)='', IF(TRIM(CCLINE2)='', TRIM(CCLINE3), TRIM(CCLINE2)), TRIM(CCNAME)), BILLCYCLE, A.CUSTOMER_ID;

select distinct original_file_date, cust_id customer_id, b.account_number custcode, null ohstatus, b.bill_date ohentdate,
    b.invoice_number ohrefnum, b.remaining_amount/100 as balance
 from cdr.spark_IT_BILL b WHERE original_file_date = '2020-04-13' and account_number='4.1090.14'
 order by bill_date desc