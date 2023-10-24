INSERT INTO MON.SPARK_FT_CXD_RECONCIALIATION_AFM
SELECT 
    numero,
    transfer_id,
    date_debit,
    date_depot,
    current_timestamp insert_date,
    EVENT_DATE
FROM
    (select
        A.sender_msisdn numero,
        A.transfer_id,
        A.date_debit,
        IF(C.date_depot is not null,C.date_depot,B.date_depot)date_depot,
        A.EVENT_DATE
    from
        (select 
            transfer_id,
            sender_msisdn, 
            receiver_msisdn, 
            transaction_amount, 
            transfer_datetime_nq date_debit,
            transfer_datetime EVENT_DATE
        from cdr.spark_it_omny_transactions
        where transfer_datetime='###SLICE_VALUE###'
        and transfer_id in
        (select trim(upper(transfer_id)) from cdr.spark_it_omny_transactions where transfer_datetime='###SLICE_VALUE###' and upper(trim(transfer_status))='TS' 
        and fn_format_msisdn_to_9digits(receiver_msisdn) in (select trim(msisdn) from dim.ref_compte_afd)
        minus
        (select trim(transactionsn) from CDR.SPARK_IT_ZTE_SUBSCRIPTION where WHERE CREATEDDATE between '###SLICE_VALUE###' and date_add('###SLICE_VALUE###', 1))
        )) A
        left join 
        (select 
            receiver_msisdn sender_msisdn, 
            sender_msisdn receiver_msisdn,
            transaction_amount,
            transfer_datetime_nq date_depot 
        from cdr.spark_it_omny_transactions 
        where transfer_datetime='###SLICE_VALUE###' and upper(txnmode) like '%ROLLBACKHELPER%')B
        ON A.sender_msisdn = B.sender_msisdn and A.receiver_msisdn = B.receiver_msisdn and A.transaction_amount = B.transaction_amount
        left join 
        (select 
            receiver_msisdn sender_msisdn, 
            sender_msisdn receiver_msisdn,
            transaction_amount,
            transfer_datetime_nq date_depot 
        from CDR.SPARK_IT_OMNY_TRANSACTIONS_HOURLY 
        where file_time=concat(date_add('###SLICE_VALUE###', 1),' 0000') and upper(txnmode) like '%ROLLBACKHELPER%')C
        ON A.sender_msisdn = C.sender_msisdn and A.receiver_msisdn = C.receiver_msisdn and A.transaction_amount = C.transaction_amount

    union

    select
        P3.numero,
        P3.transfer_id,
        P3.date_debit,
        P4.nq_createddate date_depot,
        P3.EVENT_DATE
    from 
        (SELECT 
            numero,
            transfer_id,
            date_debit,
            date_depot,
            EVENT_DATE
        FROM MON.SPARK_FT_CXD_RECONCIALIATION_AFM 
        WHERE EVENT_DATE=DATE_SUB('###SLICE_VALUE###',1) AND DATE_DEPOT is null)P3
        left join
        (select 
            trim(transactionsn)transactionsn,
            nq_createddate 
        from CDR.SPARK_IT_ZTE_SUBSCRIPTION 
        WHERE CREATEDDATE=date_add('###SLICE_VALUE###', 1))P4
        ON P3.transfer_id=trim(upper(P4.transactionsn))
    )RES