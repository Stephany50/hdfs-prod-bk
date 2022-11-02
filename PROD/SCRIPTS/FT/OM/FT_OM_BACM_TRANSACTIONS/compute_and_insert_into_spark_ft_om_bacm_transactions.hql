insert into mon.spark_ft_om_bacm_transactions
select
    SENDER_MSISDN
    , RECEIVER_MSISDN
    , SERVICE_TYPE
    , sender_user_name NAME_SENDER
    , receiver_user_name NAME_RECEIVER
    , transfer_id ID_TANGO
    , account_status STATUT
    , VAL
    , COMMISSION
    , '###SLICE_VALUE###' EVENT_DATE
from
(
    select
        USERS.msisdn msisdn
    from
    (
        select *
        from CDR.SPARK_IT_OM_ALL_USERS U
        where original_file_date = '###SLICE_VALUE###'
    ) USERS
    left join DIM.SPARK_DT_MSISDN_OM_HEADS DIM
    on fn_format_msisdn_to_9digits(trim(USERS.owner_msisdn)) = fn_format_msisdn_to_9digits(trim(DIM.msisdn))
    where DIM.msisdn is not null
) ALL_USERS
left join
(
    SELECT 
        SENDER_MSISDN 
        , RECEIVER_MSISDN
        , SERVICE_TYPE
        , sender_user_name
        , receiver_user_name
        , TRANSFER_ID
        , TRANSFER_STATUS
        , SUM(TRANSACTION_AMOUNT) VAL
        , SUM(COMMISSIONS_PAID) COMMISSION
    FROM CDR.SPARK_IT_OMNY_TRANSACTIONS
    WHERE TRANSFER_DATETIME = '###SLICE_VALUE###' 
    GROUP BY 
        RECEIVER_MSISDN 
        , SENDER_MSISDN
        , SERVICE_TYPE
        , sender_user_name
        , receiver_user_name
        , TRANSFER_ID
        , TRANSFER_STATUS

) TRANS_OM
on fn_format_msisdn_to_9digits(trim(ALL_USERS.msisdn)) in (fn_format_msisdn_to_9digits(trim(TRANS_OM.SENDER_MSISDN)), fn_format_msisdn_to_9digits(trim(TRANS_OM.RECEIVER_MSISDN))) 
where SENDER_MSISDN is not null and RECEIVER_MSISDN is not null
