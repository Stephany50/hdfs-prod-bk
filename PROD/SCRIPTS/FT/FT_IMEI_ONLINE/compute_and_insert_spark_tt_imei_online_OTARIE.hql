INSERT INTO TMP.TT_FT_OTARIE_IMEI_ONLINE
select
    imei
    , NULL IMSI
    , msisdn
    , 'OTARIE|' SRC_TABLE
    , count(*) NBRE
    , CURRENT_TIMESTAMP INSERT_DATE
    ,transaction_date sdate
from MON.SPARK_FT_OTARIE_DATA_TRAFFIC_DAY
where transaction_date = '###SLICE_VALUE###'
group by
transaction_date,
msisdn,
imei