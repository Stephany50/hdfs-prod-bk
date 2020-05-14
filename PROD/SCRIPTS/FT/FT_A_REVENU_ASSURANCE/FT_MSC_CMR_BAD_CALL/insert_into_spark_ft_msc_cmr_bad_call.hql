INSERT INTO MON.SPARK_FT_MSC_CMR_BAD_CALL
select
distinct
mois
   ,transaction_time
    ,transaction_direction
    ,transaction_type
    ,served_msisdn
    ,MSISDN_OPERATOR
    ,served_party_location
    ,other_party
    ,OTHER_OPERATOR
    ,partner_gt
    ,transaction_duration
    ,trunck_in
    ,FAISCEAU_IN
    ,trunck_out
    , FAISCEAU_OUT
    ,msc_adress
    ,transaction_service_code
    ,record_type
    ,old_calling_number
    , old_calling_operator
    ,old_called_number
    , old_called_operator
    ,roaming_number
    ,(case when length(other_party) < 9  then 'Local_Operator'
    else d.code_operateur
    end) as operator_prefix
    ,transaction_date
from
(
    select distinct
    from_unixtime(cast(unix_timestamp(transaction_date,'YYYYMM') as bigint),'YYYYMM') mois
    ,transaction_time
    ,transaction_direction
    ,transaction_type
    ,served_msisdn
    ,FN_GET_NNP_MSISDN_SIMPLE_DESTN(SERVED_MSISDN) as MSISDN_OPERATOR
    ,served_party_location
    ,other_party
    ,(case when length(other_party) < 9 then SUBSTR(TRUNCK_IN, 1, 5)||'_SHORT'
    else FN_GET_NNP_MSISDN_SIMPLE_DESTN(OTHER_PARTY)
    end) as OTHER_OPERATOR
    ,partner_gt
    ,transaction_duration
    ,trunck_in
    ,SUBSTR(TRUNCK_IN, 1, 5) AS FAISCEAU_IN
    ,trunck_out
    ,SUBSTR(TRUNCK_OUT, 1, 5) AS FAISCEAU_OUT
    ,msc_adress
    ,transaction_service_code
    ,record_type
    ,old_calling_number
    ,FN_GET_NNP_MSISDN_SIMPLE_DESTN(old_calling_number) as old_calling_operator
    ,old_called_number
    ,FN_GET_NNP_MSISDN_SIMPLE_DESTN(old_called_number) as old_called_operator
    ,roaming_number
    ,current_timestamp insert_date
    ,transaction_date
    from
    (SELECT * FROM MON.SPARK_FT_MSC_TRANSACTION
    WHERE transaction_date = "###SLICE_VALUE###"
    and transaction_type <> 'TEL_CFW'
    and substr(transaction_type, 1, 3) = 'TEL'
    and other_party not like '160%') a
    INNER JOIN
    (SELECT * FROM DIM.SPARK_DT_REF_OPERATEURS
    WHERE NVL(prefix_trunck, '0') <> '0' and country_name = 'CAMEROON' ) b
    ON substr(a.trunck_in, 1, 5) = b.prefix_trunck
    where
    NOT (substr(NVL(a.other_party, 0), 1, 2) IN
    (select distinct substr(ncc, 1, 2) from DIM.SPARK_DT_REF_OPERATEURS
    where b.prefix_trunck = substr(a.trunck_in, 1, 5))
    and length(NVL(a.other_party, 0)) = 9)
) T
left join
(SELECT cc,ncc,cc||ncc code_operateur  FROM DIM.SPARK_DT_REF_OPERATEURS) d
on T.other_party like d.CC||d.NCC||'%'