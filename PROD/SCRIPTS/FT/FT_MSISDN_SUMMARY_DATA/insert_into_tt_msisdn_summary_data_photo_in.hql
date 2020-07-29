insert  into TMP.TT_MSISDN_SUMMARY_DATA_PHOTO_IN
select
    MSISDN,
    COUVERTURE,
    TECHNOLOGIE,
    STATUS,
    FIRST_COUV_4G ,
    LAST_COUV_4G ,
    COUNT_COUV_4G ,
    FIRST_COUV_3G ,
    LAST_COUV_3G ,
    COUNT_COUV_3G ,
    FIRST_COUV_2G ,
    LAST_COUV_2G ,
    COUNT_COUV_2G ,
    FIRST_PHONE_4G ,
    LAST_PHONE_4G ,
    COUNT_PHONE_4G ,
    FIRST_PHONE_3G ,
    LAST_PHONE_3G ,
    COUNT_PHONE_3G ,
    FIRST_PHONE_2G ,
    LAST_PHONE_2G ,
    COUNT_PHONE_2G ,
    FIRST_USING_DATA ,
    LAST_USING_DATA ,
    COUNT_USING_DATA ,
    BYTES_USED ,
    MAX_BYTES_USED ,
    DATE_MAX_BYTES_USED ,
    nvl(D.OSP_STATUS,S.osp_status) OSP_STATUS,
    OTARIE_FIRST_RAT ,
    OTARIE_BYTES_2G ,
    OTARIE_BYTES_3G ,
    OTARIE_BYTES_4G ,
    OTARIE_BYTES_UKN ,
    LAST_IMEI_USED ,
    S.EVENT_DATE ,
    INSERT_DATE
from TMP.TT_MSISDN_SUMMARY_DATA S
LEFT JOIN (
    select
        event_date,
        access_key,
        max(osp_status) osp_status
    from MON.SPARK_ft_contract_snapshot
    where event_date = '2020-04-01'    --'02/11/2016'
    group by event_date, access_key
) D on msisdn = access_key

