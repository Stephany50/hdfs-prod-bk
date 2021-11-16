select distinct 
    a.USER_LAST_NAME NOM
    , a.USER_FIRST_NAME PRENOM
    , a.BIRTH_DATE DATE_NAISSANCE
    , a.ID_NUMBER NUMERO_CNI
    , a.MSISDN MSISDN
    , case when c.msisdn is not null then 'OUI'else 'NON' end EST_ACTIF_OM
    , Activation_date_TEL 
    , REGISTERED_ON Created_date_OM
from 
(    
    select 
        a.*
        , b.activation_date Activation_date_TEL
    from 
    (
        select
            user_id
            , account_number
            , msisdn
            , user_first_name
            , user_last_name
            , registered_on
            , city
            , address
            , sex
            , id_number
            , account_status
            , created_by
            , first_transaction_on
            , user_domain
            , user_category
            , geographical_domain
            , user_type
            , deleted_on
            , deactivation_by
            , bill_company_code
            , company_type
            , notification_type
            , profile_id
            , parent_user_id
            , creation_date
            , modified_by
            , modified_on
            , birth_date
            , account_balance
            , created_by_msisdn
        from
        (
            select a.*
                , row_number() OVER (PARTITION BY a.Msisdn ORDER BY REGISTERED_ON DESC) rang
            from mon.spark_ft_omny_account_snapshot a
            WHERE EVENT_DATE = '###SLICE_VALUE###' 
                AND USER_DOMAIN='Subscriber'
        ) a
        where rang  = 1
    ) a
    left join
    (
        select * from mon.spark_FT_CONTRACT_SNAPSHOT where EVENT_DATE='###SLICE_VALUE###'
    ) b on a.MSISDN=b.ACCESS_KEY
) a
left join
(
    select distinct msisdn
    from mon.spark_FT_OM_ACTIVE_USER
    where event_date in ('###SLICE_VALUE###', date_sub('###SLICE_VALUE###', 30), date_sub('###SLICE_VALUE###', 60)) 
) c on a.msisdn =c.msisdn
