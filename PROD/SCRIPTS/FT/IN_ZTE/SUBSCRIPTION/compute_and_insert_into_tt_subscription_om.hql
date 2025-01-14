insert into tt.subscription_om_2
select
    sender_msisdn msisdn,
    price_plan_code,
    transaction_amount,
    A.transfer_id transfer_id
from
(
    select * from cdr.spark_it_omny_transactions 
    where transfer_datetime between date_sub('###SLICE_VALUE###', 1) and '###SLICE_VALUE###'  and upper(trim(transfer_status))='TS'
) A
right join
(
    select
        SUBSTRING(ACC_NBR, -9) MSISDN,
        transactionsn,
        max(PRICE_PLAN_CODE) PRICE_PLAN_CODE,
        max(NQ_CREATEDDATE) NQ_CREATEDDATE,
        CREATEDDATE
    from
    (
        SELECT *
        FROM CDR.SPARK_IT_ZTE_SUBSCRIPTION A
        WHERE A.CREATEDDATE = '###SLICE_VALUE###' AND original_file_name not like '%in_postpaid%' and upper(trim(transactionsn)) like 'MP%' 
        and cast(price_plan_code as int) not in (51104810, 51104811, 51104812, 51104813)
    ) T
    group by 
        ACC_NBR, 
        transactionsn,
        CREATEDDATE
) B
on upper(trim(A.transfer_id))=upper(trim(B.transactionsn)) 
where A.transfer_id is not null