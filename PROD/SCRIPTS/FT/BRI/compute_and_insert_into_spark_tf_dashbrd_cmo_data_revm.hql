   INSERT INTO MON.SPARK_TF_DASHBRD_CMO_DATA_REVM
    SELECT nvl(b.ADMINISTRATIVE_REGION,'') ADMINISTRATIVE_REGION
        , nvl(b.COMMERCIAL_REGION,'') COMMERCIAL_REGION
        , nvl(b.TOWNNAME,'') TOWNNAME
        , nvl(b.SITE_NAME,'') SITE_NAME
        , nvl(e.CONSTRUCTOR, '') CONSTRUCTOR_HANDSET
        , nvl(e.DATA_COMPATIBLE,'') DATA_COMPATIBLE_HANDSET
        , sum(1) total_data_doer
        , sum(CASE WHEN MAIN_RATED_AMOUNT + GOS_DEBIT_AMOUNT > 0 THEN 1 ELSE 0 end) total_rev_provider
        , sum(CASE WHEN MAIN_RATED_AMOUNT_ROAMING > 0 THEN 1 ELSE 0 end) total_roam_rev_provider
        , sum(CASE WHEN GOS_DEBIT_AMOUNT > 0 THEN 1 ELSE 0 end) total_gossdp_rev_provider
        , sum(CASE WHEN BYTES_RECEIVED > 0 THEN 1 ELSE 0 end) total_byte_receiver
        , sum(CASE WHEN BYTES_SENT > 0 THEN 1 ELSE 0 end) total_byte_sender
        , sum(CASE WHEN BYTES_USED_IN_BUNDLE > 0 THEN 1 ELSE 0 end) total_byte_user_in_bundle
        , sum(CASE WHEN BYTES_USED_OUT_BUNDLE > 0 THEN 1 ELSE 0 end) total_byte_user_out_bundle
        , sum(CASE WHEN BYTES_USED_IN_BUNDLE_ROAMING > 0 THEN 1 ELSE 0 end) total_byte_user_in_bundleroam
        , sum(CASE WHEN BYTES_USED_OUT_BUNDLE_ROAMING > 0 THEN 1 ELSE 0 end) total_byte_user_out_bundleroam
        , sum(CASE WHEN MMS_COUNT > 0 THEN 1 ELSE 0 end) total_mms_user
        , sum(CASE WHEN BUNDLE_MMS_USED_VOLUME > 0 THEN 1 ELSE 0 end) total_mms_user_in_bundle
        , sum(MAIN_RATED_AMOUNT + GOS_DEBIT_AMOUNT) MAIN_CONSO
        , sum(PROMO_RATED_AMOUNT) PROMO_CONSO
        , sum(MAIN_RATED_AMOUNT_ROAMING) ROAM_MAIN_CONSO
        , sum(PROMO_RATED_AMOUNT_ROAMING) ROAM_PROMO_CONSO
        , sum(BYTES_RECEIVED) BYTES_RECEIVED
        , sum(BYTES_SENT) BYTES_SENT
        , sum(BYTES_USED_IN_BUNDLE) BYTES_USED_IN_BUNDLE
        , sum(BYTES_USED_OUT_BUNDLE) BYTES_USED_OUT_BUNDLE
        , sum(BYTES_USED_IN_BUNDLE_ROAMING) BYTES_USED_IN_BUNDLE_ROAMING
        , sum(BYTES_USED_OUT_BUNDLE_ROAMING) BYTES_USED_OUT_BUNDLE_ROAMING
        , sum(MMS_COUNT) MMS_COUNT
        , sum(BUNDLE_MMS_USED_VOLUME) BUNDLE_MMS_USED_VOLUME
        , sum(GOS_DEBIT_COUNT) GOS_DEBIT_COUNT
        , sum(GOS_DEBIT_AMOUNT) GOS_DEBIT_AMOUNT
        , CURRENT_TIMESTAMP INSERT_DATE
,EVENT_MONTH
    FROM (SELECT * FROM MON.SPARK_FT_DATA_CONSO_MSISDN_MONTH  WHERE EVENT_MONTH = '###SLICE_VALUE###') a
        left join ( select MSISDN
                    , SITE_NAME
                    , TOWNNAME
                    , ADMINISTRATIVE_REGION
                    , COMMERCIAL_REGION
                from MON.SPARK_FT_CLIENT_LAST_SITE_LOCATION b
                where b.EVENT_MONTH = '###SLICE_VALUE###'
         ) b
           on a.MSISDN = b.MSISDN
         left join (
            SELECT substr(e.imei,1,8) tac_code_handset, e.*, f.*
            FROM
                (SELECT a.*
                    , ROW_NUMBER() OVER(PARTITION BY MSISDN ORDER BY TOTAL_DAYS_COUNT DESC) AS IMEI_RN
                  FROM MON.SPARK_FT_IMEI_TRAFFIC_MONTHLY a
                  WHERE smonth = '###SLICE_VALUE###'
                    ) e
                left join
                 (select a.*
                    , (CASE WHEN UMTS = 'O' or GPRS = 'O' or EDGE = 'O' or EDGE = 'E' or LTE = 'O' THEN 'YES' ELSE 'NO' END) DATA_COMPATIBLE
                  from dim.spark_dt_handset_ref a
                    ) f
            on substr(e.imei,1,8) = f.tac_code
              where e.IMEI_RN = 1
            ) e
        on a.MSISDN = e.MSISDN
    GROUP BY a.EVENT_MONTH
        , nvl(b.ADMINISTRATIVE_REGION,'')
        , nvl(b.COMMERCIAL_REGION,'')
        , nvl(b.TOWNNAME,'')
        , nvl(b.SITE_NAME,'')
        , nvl(e.CONSTRUCTOR, '')
        , nvl(e.DATA_COMPATIBLE,'')