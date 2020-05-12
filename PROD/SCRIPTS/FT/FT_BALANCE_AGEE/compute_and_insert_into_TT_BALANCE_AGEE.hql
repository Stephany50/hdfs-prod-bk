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
        where original_file_date between '2020-04-20' and '2020-05-01' --and AcctGuid is null

            and nvl(total_bill_amount, 0) != 0 --and bill_cycle_id in ('to_define')
        group by original_file_date, custid, guid, custseg, custname, firstname, lastname
) a LEFT JOIN (
    select distinct original_file_date, cust_id customer_id, b.account_number custcode, null ohstatus, b.bill_date ohentdate, b.invoice_number ohrefnum, b.remaining_amount/100 as balance
    from cdr.spark_IT_BILL b
    where original_file_date between '2020-04-20' and '2020-05-01'

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
WHERE EVENT_DATE between '2020-04-20' and '2020-05-01'

GROUP BY EVENT_DATE, CUSTCODE, PRGCODE, IF(TRIM(CCNAME)='', IF(TRIM(CCLINE2)='', TRIM(CCLINE3), TRIM(CCLINE2)), TRIM(CCNAME)), BILLCYCLE, A.CUSTOMER_ID;

select distinct original_file_date, cust_id customer_id, b.account_number custcode, null ohstatus, b.bill_date ohentdate,
    b.invoice_number ohrefnum, b.remaining_amount/100 as balance
 from cdr.spark_IT_BILL b WHERE original_file_date = '2020-04-13' and account_number='4.1090.14'
 order by bill_date desc

-------- change finaux -------------------------------
insert into TMP.TMP_CUST_CONTACT_ORDER
SELECT a.customer_id, custcode, prgcode, ccname, ccline2, ccline3,  cast(ohstatus as string) ohstatus, ohentdate, ohrefnum, balance, billcycle,
    (CASE WHEN b.customer_id IS NOT NULL THEN balance ELSE 0 END) AS balance_prev_period, cscurbalance balance_current,
    (CASE WHEN b.customer_id IS NOT NULL and ohrefnum IS NOT NULL THEN ohentdate ELSE NULL END) AS bill_date, a.original_file_date event_date, current_timestamp insert_date
FROM (
        select original_file_date, custid customer_id, guid, acctguid custcode, custseg prgcode, custname ccname, firstname ccline2, lastname ccline3,
            max(BILL_CYCLE_ID) BILLCYCLE, sum(total_bill_amount/100) cscurbalance, count(*) nbre_entree
        from CDR.SPARK_IT_CUST_FULL
        where original_file_date between '2020-05-03' and '2020-05-10'
            --and AcctGuid is not null
            and nvl(total_bill_amount, 0) != 0 --and bill_cycle_id in ('to_define')
        group by original_file_date, custid, guid, acctguid, custseg, custname, firstname, lastname
) a LEFT JOIN (
    select distinct original_file_date, cust_id customer_id, b.account_number, null ohstatus, b.bill_date ohentdate, b.invoice_number ohrefnum, b.remaining_amount/100 as balance, bill_amount/100 bill_amount
    from cdr.spark_IT_BILL b
    where original_file_date between  '2020-05-03' and '2020-05-10'
) b  ON a.custcode = b.account_number and a.original_file_date = b.original_file_date

INSERT INTO TMP.TT_TMP_BALANCE_AGEE
--create table TMP.TT_TMP_BALANCE_AGEE as
SELECT a.EVENT_DATE, CUSTCODE AS CODE_CLIENT, PRGCODE AS CATEGORIE,
        IF(TRIM(ccname)='', IF(TRIM(ccline2)='', TRIM(ccline3), TRIM(ccline2)), TRIM(ccname)) as NOM,
        SUM(nvl(balance,0.00)) AS balance, MAX(bill_date) AS derniere_facture,
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
        billcycle AS BILLCYCLE_CODE, a.CUSTOMER_ID, max(balance_current) balance_current, count(distinct ohrefnum) nbre_facture, count(*) nbre_entree, current_timestamp AS INSERT_DATE
FROM TMP.TMP_CUST_CONTACT_ORDER A
WHERE EVENT_DATE between '2020-05-03' and '2020-05-10'
GROUP BY EVENT_DATE, CUSTCODE, PRGCODE, IF(TRIM(CCNAME)='', IF(TRIM(CCLINE2)='', TRIM(CCLINE3), TRIM(CCLINE2)), TRIM(CCNAME)), BILLCYCLE, A.CUSTOMER_ID

INSERT INTO TMP.TT_BALANCE_AGEE
--create table TMP.TT_BALANCE_AGEE2 as
SELECT CODE_CLIENT, CATEGORIE, NOM, CASE WHEN count(case when prod_state='Active' then accnbr end) !=0 THEN 'ACTIF'
    WHEN count(case when prod_state in ('Termination', 'One-Way Block', 'Two-Way Block') then accnbr end) = 0 then 'DESACTIVE' ELSE 'SUSPENDU' END STATUT,
BILLCYCLE_CODE, BALANCE, DERNIERE_FACTURE, balance_J, balance_J_30, balance_J_60, balance_J_90, balance_J_120, balance_J_150, balance_J_180,
 balance_J_360, balance_J_720, balance_J_720_Plus, a.CUSTOMER_ID, BALANCE_CURRENT, NBRE_FACTURE, NBRE_ENTREE, count(distinct accnbr) NBRE_FILS,
 count(case when prod_state='Active' then accnbr end) nbre_actif, count(case when prod_state in ('Non-provisioning', 'Inactive') or prod_state is null then accnbr end) nbre_inactif, count(case when prod_state in ('Termination', 'One-Way Block', 'Two-Way Block') then accnbr end) nbre_suspendu, CURRENT_TIMESTAMP INSERT_DATE, EVENT_DATE
FROM TMP.TT_TMP_BALANCE_AGEE A
LEFT JOIN (
    SELECT original_file_date, accnbr, RESP_PAYMENT, ACCOUNT_NUMBER, PARENT_ACCOUNT_NUMBER, prod_state, block_reason, order_reason, update_date, activation_date, iccid, imsi,
        customer_id, subscriber_type, default_price_plan_id, subs_id, row_number()over( partition by original_file_date, accnbr order by cast(subs_id as int) desc, update_date desc) rn_cont
    FROM CDR.SPARK_IT_CONT WHERE original_file_date between '2020-05-03' and '2020-05-10'
        and subscriber_type !='VPN(OCS)'
) b on a.CODE_CLIENT = b.RESP_PAYMENT and b.original_file_date= a.event_date and rn_cont=1
WHERE EVENT_DATE between '2020-05-03' and '2020-05-10'
GROUP BY EVENT_DATE, CODE_CLIENT, CATEGORIE, NOM, BALANCE, DERNIERE_FACTURE, balance_J, balance_J_30, balance_J_60, balance_J_90, balance_J_120, balance_J_150, balance_J_180,
          balance_J_360, balance_J_720, balance_J_720_Plus, BILLCYCLE_CODE, a.CUSTOMER_ID, BALANCE_CURRENT, NBRE_FACTURE, NBRE_ENTREE
LIMIT 5;

select original_file_date, count(*), count(distinct account_number), count(distinct resp_payment), count(distinct parent_account_number) from (SELECT original_file_date, accnbr, RESP_PAYMENT, ACCOUNT_NUMBER, PARENT_ACCOUNT_NUMBER, prod_state, block_reason, order_reason, update_date, activation_date, iccid, imsi, customer_id, subscriber_type, default_price_plan_id, subs_id, row_number()over(partition by original_file_date, accnbr order by cast(subs_id as int) desc, update_date desc) rn_cont
FROM CDR.SPARK_IT_CONT WHERE original_file_date >='2020-04-17') a where rn_cont=1  group by original_file_date

select event_date, statut, count(*), sum(NBRE_FILS), insert_date from TMP.TT_BALANCE_AGEE2 group by statut, event_date, insert_date order by insert_date;

select event_date, count(*), sum(nbre_entree), insert_date from TMP.TT_TMP_BALANCE_AGEE group by event_date, insert_date order by insert_date;

select event_date, count(*), sum(nbre_entree), insert_date from TMP.TT_BALANCE_AGEE2 group by event_date, insert_date order by insert_date;

create table tmp.tt_msisdn_balance_agee as
insert into tmp.tt_msisdn_balance_agee
select a.event_date, a.code_client, NOM, BALANCE, DERNIERE_FACTURE, b.*, current_timestamp insert_date
FROM TMP.TT_TMP_BALANCE_AGEE A
LEFT JOIN (
    SELECT original_file_date, accnbr, RESP_PAYMENT, ACCOUNT_NUMBER, PARENT_ACCOUNT_NUMBER, prod_state, block_reason, order_reason, update_date, activation_date, iccid, imsi, customer_id, subscriber_type, default_price_plan_id, subs_id, row_number()over(partition by accnbr order by cast(subs_id as int) desc, update_date desc) rn_cont
    FROM CDR.SPARK_IT_CONT WHERE original_file_date ='2020-05-01' and subscriber_type !='VPN(OCS)'
) b on a.CODE_CLIENT = b.RESP_PAYMENT and rn_cont=1
WHERE EVENT_DATE ='2020-05-01'

INSERT INTO TMP.TT_BALANCE_AGEE2
--create table TMP.TT_BALANCE_AGEE2 as
SELECT a.CODE_CLIENT, CATEGORIE, NOM, IF(NBRE_ACTIF=0, IF(NBRE_SUSPENDU=0, 'DESACTIVE', 'SUSPENDU'), 'ACTIF') STATUT,
BILLCYCLE_CODE, BALANCE, DERNIERE_FACTURE, balance_J, balance_J_30, balance_J_60, balance_J_90, balance_J_120, balance_J_150, balance_J_180,
 balance_J_360, balance_J_720, balance_J_720_Plus, a.CUSTOMER_ID, BALANCE_CURRENT, NBRE_FACTURE, NBRE_ENTREE, NBRE_FILS,
 NBRE_ACTIF, NBRE_INACTIF, NBRE_SUSPENDU, CURRENT_TIMESTAMP INSERT_DATE, a.EVENT_DATE
FROM TMP.TT_TMP_BALANCE_AGEE A
LEFT JOIN (
    SELECT EVENT_DATE, CODE_CLIENT, COUNT(*) NBRE_FILS, COUNT(CASE WHEN PROD_STATE='Active' THEN ACCNBR END) NBRE_ACTIF,
        COUNT(CASE WHEN PROD_STATE IN ('Non-provisioning', 'Inactive') OR PROD_STATE IS NULL THEN ACCNBR END) NBRE_INACTIF,
        COUNT(CASE WHEN PROD_STATE IN ('Termination', 'One-Way Block', 'Two-Way Block') THEN ACCNBR END) NBRE_SUSPENDU
    FROM TMP.TT_MSISDN_BALANCE_AGEE WHERE EVENT_DATE ='2020-05-02'
    GROUP BY EVENT_DATE, CODE_CLIENT ORDER BY EVENT_DATE, CODE_CLIENT
) b on a.CODE_CLIENT = b.CODE_CLIENT
WHERE a.EVENT_DATE ='2020-05-02'
GROUP BY a.EVENT_DATE, a.CODE_CLIENT, CATEGORIE, NOM, BALANCE, DERNIERE_FACTURE, balance_J, balance_J_30, balance_J_60, balance_J_90, balance_J_120, balance_J_150, balance_J_180,
          balance_J_360, balance_J_720, balance_J_720_Plus, BILLCYCLE_CODE, a.CUSTOMER_ID, BALANCE_CURRENT, NBRE_FACTURE, NBRE_ENTREE, NBRE_FILS, NBRE_ACTIF, NBRE_INACTIF, NBRE_SUSPENDU
;
