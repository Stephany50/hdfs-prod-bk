insert into tt.subscription_om
select
    sender_msisdn msisdn,
    price_plan_code,
    transaction_amount
from
(
    select * from cdr.spark_it_omny_transactions 
    where transfer_datetime='###SLICE_VALUE###' and upper(trim(transfer_status))='TS'
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
    ) T
    group by 
        ACC_NBR, 
        transactionsn,
        CREATEDDATE
) B
on upper(trim(A.transfer_id))=upper(trim(B.transactionsn)) 
where A.transfer_id is not null