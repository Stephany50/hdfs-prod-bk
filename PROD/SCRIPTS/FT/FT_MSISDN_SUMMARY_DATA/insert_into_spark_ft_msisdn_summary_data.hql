INSERT INTO MON.SPARK_FT_MSISDN_SUMMARY_DATA
select
MSISDN,
COUVERTURE,
TECHNOLOGIE,
STATUS,
FIRST_COUV_4G,
LAST_COUV_4G,
COUNT_COUV_4G,
FIRST_COUV_3G,
LAST_COUV_3G,
COUNT_COUV_3G,
FIRST_COUV_2G,
LAST_COUV_2G,
COUNT_COUV_2G,
FIRST_PHONE_4G,
LAST_PHONE_4G,
COUNT_PHONE_4G,
FIRST_PHONE_3G,
LAST_PHONE_3G,
COUNT_PHONE_3G,
FIRST_PHONE_2G,
LAST_PHONE_2G,
COUNT_PHONE_2G,
FIRST_USING_DATA,
LAST_USING_DATA,
COUNT_USING_DATA,
BYTES_USED,
MAX_BYTES_USED,
DATE_MAX_BYTES_USED,
OSP_STATUS,
OTARIE_FIRST_RAT,
OTARIE_BYTES_2G,
OTARIE_BYTES_3G,
OTARIE_BYTES_4G,
OTARIE_BYTES_UKN,
LAST_IMEI_USED,
INSERT_DATE,
EVENT_DATE
from (
    SELECT
        NVL(A.MSISDN,Src.MSISDN) msisdn,
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
        OSP_STATUS,
        OTARIE_FIRST_RAT ,
        NVL(A.OTARIE_BYTES_2G,Src.Otarie_Bytes_2G)OTARIE_BYTES_2G , --- les elements sont null dans A
        NVL(A.OTARIE_BYTES_3G,Src.Otarie_Bytes_3G)OTARIE_BYTES_3G,
        NVL(A.OTARIE_BYTES_4G,Src.Otarie_Bytes_4G)OTARIE_BYTES_4G,
        NVL(A.OTARIE_BYTES_UKN,Src.Otarie_Bytes_Ukn)OTARIE_BYTES_UKN ,
        LAST_IMEI_USED ,
        nvl(a.EVENT_DATE,SRC.TRANSACTION_DATE) event_date ,
        CURRENT_TIMESTAMP INSERT_DATE
    FROM TMP.TT_MSISDN_SUMMARY_DATA_PHOTO_IN A
    FULL OUTER JOIN  (
            select TRANSACTION_DATE , MSISDN
                , sum(case when RADIO_ACCESS_TECHNO = '2G'
                        then Nbytest else 0
                        end) Otarie_BYTES_2G
                , sum(case when RADIO_ACCESS_TECHNO in ('3G', 'HSPA') then Nbytest else 0 end) Otarie_Bytes_3G
                , sum(case when RADIO_ACCESS_TECHNO = 'LTE' then Nbytest else 0 end) Otarie_Bytes_4G
                , sum(case when RADIO_ACCESS_TECHNO = 'Unknown' then Nbytest else 0 end) Otarie_Bytes_Ukn
            from MON.SPARK_FT_OTARIE_DATA_TRAFFIC_DAY
            where transaction_date = '###SLICE_VALUE###'  --'03/11/2016'
            group by  TRANSACTION_DATE, MSISDN
    ) Src on  A.msisdn = Src.msisdn
)T

