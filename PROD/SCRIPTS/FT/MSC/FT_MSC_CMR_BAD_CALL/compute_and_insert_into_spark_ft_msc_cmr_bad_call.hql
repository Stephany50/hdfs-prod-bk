INSERT INTO MON.SPARK_FT_MSC_CMR_BAD_CALL
   select distinct
   from_unixtime(cast(unix_timestamp(transaction_date,'YYYYMM') as bigint),'YYYYMM') mois
   ,transaction_time
   ,transaction_direction
   ,transaction_type
   ,served_msisdn
   ,MSISDN_OPERATOR
   ,served_party_location
   ,other_party
   ,(case when length(other_party) < 8 then SUBSTR(TRUNCK_IN, 1, 5)||'_SHORT'
        else MON.FN_GET_NNP_MSISDN_SIMPLE_DESTN(OTHER_PARTY)
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
   ,MON.FN_GET_NNP_MSISDN_SIMPLE_DESTN(old_calling_number) as old_calling_operator
   ,old_called_number
   ,MON.FN_GET_NNP_MSISDN_SIMPLE_DESTN(old_called_number) as old_called_operator
   ,roaming_number
   ,(case when length(other_party) < 8  then 'Local_Operator'
        else MON.fn_iw_numbering_plan('00'||other_party, 'code_operateur')
       end) as operator_prefix, transaction_date
    from MON.SPARK_FT_MSC_TRANSACTION a
    , dim.dt_Ref_Operateurs
    where transaction_date = "###SLICE_VALUE###"      --'20/01/2017'
    and substr(transaction_type, 1, 3) = 'TEL'
    and transaction_type <> 'TEL_CFW'
    and country_name = 'CAMEROON'
    and other_party not like '160%'
    and NVL(prefix_trunck, '0') <> '0'
    and substr(trunck_in, 1, 5) = prefix_trunck
    and not (substr(NVL(other_party, 0), 1, 2) in (
        select distinct substr(ncc, 1, 2) from dim.dt_ref_operateurs
        where prefix_trunck = substr(a.trunck_in, 1, 5)
    ) and length(NVL(other_party, 0)) = 9-- is null
 );
