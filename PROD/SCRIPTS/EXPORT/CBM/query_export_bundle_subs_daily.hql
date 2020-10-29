SELECT 
    from_unixtime(unix_timestamp(PERIOD), 'dd/MM/yyyy') `date`,
    msisdn,
    bdle_cost,
    nber_purchase,
    bdle_name,
    validity,
    subscription_channel,
    amount_voice_onnet,
    amount_voice_offnet,
    amount_voice_inter,
    amount_voice_roaming,
    amount_sms_onnet,
    amount_sms_offnet,
    amount_sms_inter,
    amount_sms_roaming,
    amount_data,
    amount_sva,
    benefit_bal_list,
    bal_id
FROM MON.SPARK_FT_CBM_BUNDLE_SUBS_DAILY
WHERE PERIOD = "###SLICE_VALUE###"