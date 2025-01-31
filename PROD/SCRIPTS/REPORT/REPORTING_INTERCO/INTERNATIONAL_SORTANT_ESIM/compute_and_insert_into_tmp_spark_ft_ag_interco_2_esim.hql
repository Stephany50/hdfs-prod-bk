INSERT INTO TMP.SPARK_FT_AG_INTERCO_2_ESIM
SELECT
SERVED_MSISDN
,OTHER_PARTY
,'FTMSC' table_name
, SDATE
, substr(HEURE, 1, 2) HEURE
, TRANSACTION_TYPE USAGE_APPELs
, TYPE_APPEL
, TYPE_ABONNE
, NULL TYPE_HEURE
, RECORD_TYPE
, CALLER_SUBR
, PARTNER_ID
, PARTNER_GT
, CRA_SRC
, TRANSACTION_SERVICE_CODE
, PARTNER_GT_LEN
, PARTNER_ID_LEN
, CALLER_SUBR_LEN
, ( CASE WHEN PARTNER_ID NOT IN ('BELG', 'FTLD', 'BICS')
    THEN (CASE WHEN SUBSTR (PARTNER_ID_PREFIX, 1, 3)  = '237'
        THEN IF(SUBSTR (PARTNER_ID_PREFIX, 4, 2)="",NULL,SUBSTR (PARTNER_ID_PREFIX, 4, 2))
            ELSE IF(SUBSTR (PARTNER_ID_PREFIX, 1, 2)="",NULL ,SUBSTR (PARTNER_ID_PREFIX, 1, 2))
        END )
    ELSE IF(substr(PARTNER_ID_PREFIX, 1, 9)="",NULL,substr(PARTNER_ID_PREFIX, 1, 9))
    END )  PARTNER_ID_PREFIX
, ( CASE WHEN PARTNER_GT NOT IN ('BELG', 'FTLD', 'BICS')
        THEN (CASE WHEN SUBSTR (PARTNER_GT_PREFIX, 1, 3)  = '237'
                THEN IF( SUBSTR (PARTNER_GT_PREFIX, 4, 2)="",NULL, SUBSTR (PARTNER_GT_PREFIX, 4, 2))
                ELSE IF(SUBSTR (PARTNER_GT_PREFIX, 1, 2)="",NULL,SUBSTR (PARTNER_GT_PREFIX, 1, 2))
                END )
        ELSE IF(substr(PARTNER_GT_PREFIX, 1, 9)="",NULL,substr(PARTNER_GT_PREFIX, 1, 9))
    END ) PARTNER_GT_PREFIX
, TRUNCK_OUT
, TRUNCK_IN
, SUM (CAST (NVL(TRANSACTION_DURATION,0) AS BIGINT)) TRANSACTION_DURATION
, SUM (1) CRA_COUNT
, CURRENT_TIMESTAMP INSERTED_DATE
, OLD_CALLED_NUMBER
, OLD_CALLING_NUMBER
, FN_INTERCO_DESTINATION(SERVICECENTRE)
, ( CASE WHEN SERVICECENTRE NOT IN ('BELG', 'FTLD', 'BICS')  THEN (CASE WHEN SUBSTR (SERVICECENTRE_PREFIX, 1, 3)  = '237' THEN SUBSTR (SERVICECENTRE_PREFIX, 4, 2) ELSE SUBSTR (SERVICECENTRE_PREFIX, 1, 2) END ) ELSE SERVICECENTRE_PREFIX END ) SERVICECENTRE_PREFIX
, SERVICECENTRE_LEN
, OPERATOR_CODE
, SERVED_PARTY_LOCATION MSC_LOCATION
,TO_DATE(SDATE)
FROM (
    SELECT
          SERVED_MSISDN
        , TRANSACTION_DATE SDATE
        , TRANSACTION_TIME HEURE
        , TRANSACTION_DIRECTION  TYPE_APPEL
        , substr(TRANSACTION_TYPE, 1, 3) TRANSACTION_TYPE
        , TRANSACTION_SERVICE_CODE
        , SUBSCRIBER_TYPE  TYPE_ABONNE
        , SERVED_MSISDN
        , OTHER_PARTY
        , SUBSTR (MSC_SOURCE_FILE, 1, 7) CRA_SRC
        , LENGTH ( PARTNER_GT ) PARTNER_GT_LEN
        , LENGTH ( OTHER_PARTY ) PARTNER_ID_LEN
        , LENGTH ( SERVED_MSISDN ) CALLER_SUBR_LEN
        , OTHER_PARTY   PARTNER_ID_PREFIX
        , PARTNER_GT    PARTNER_GT_PREFIX
        , FN_INTERCO_DESTINATION (SERVED_MSISDN) CALLER_SUBR
        , FN_INTERCO_DESTINATION (OTHER_PARTY) PARTNER_ID
        , FN_INTERCO_DESTINATION ( PARTNER_GT) PARTNER_GT
        , FN_INTERCO_TRUNCKNAME_DEST (MAX(TRUNCK_OUT))  TRUNCK_OUT
        , FN_INTERCO_TRUNCKNAME_DEST (MIN(TRUNCK_IN))  TRUNCK_IN
        , FN_INTERCO_DESTINATION (MAX(OLD_CALLED_NUMBER)) OLD_CALLED_NUMBER
        , FN_INTERCO_DESTINATION (MAX(OLD_CALLING_NUMBER)) OLD_CALLING_NUMBER
        , FN_GET_OPERATOR_CODE(SERVED_MSISDN) OPERATOR_CODE
        , MAX( SERVED_PARTY_LOCATION ) SERVED_PARTY_LOCATION
        , MAX(MSC_SOURCE_FILE) MSC_SOURCE_FILE
        , MAX(NVL(TRANSACTION_DURATION, 0)) TRANSACTION_DURATION
        , MIN(RECORD_TYPE) RECORD_TYPE
        , MAX( SERVICE_CENTRE ) SERVICECENTRE
        , MAX( SUBSTR ( SERVICE_CENTRE, 1, 5 )) SERVICECENTRE_PREFIX
        , MAX( LENGTH ( SERVICE_CENTRE )) SERVICECENTRE_LEN
    FROM MON.SPARK_FT_MSC_TRANSACTION
    WHERE TRANSACTION_DATE = "###SLICE_VALUE###"
    -- AND (
    --     SERVED_MSISDN in (
    --         select distinct msisdn from (
    --             select distinct msisdn,a.imsi,sdate traffic_date,event_date as sales_date
    --             from (select distinct msisdn imsi,event_date from dmc_bi.esim_sales where event_date between '2022-01-01' and '2022-05-31') a 
    --             left join (select distinct imsi,imei,msisdn,sdate from mon.spark_ft_imei_online where sdate between '2022-01-01' and '2022-05-31') b on a.imsi = b.imsi
    --         )
    --     ) OR OTHER_PARTY in (
    --         select distinct msisdn from (
    --             select distinct msisdn,a.imsi,sdate traffic_date,event_date as sales_date
    --             from (select distinct msisdn imsi,event_date from dmc_bi.esim_sales where event_date between '2022-01-01' and '2022-05-31') a 
    --             left join (select distinct imsi,imei,msisdn,sdate from mon.spark_ft_imei_online where sdate between '2022-01-01' and '2022-05-31') b on a.imsi = b.imsi
    --         )
    --     )
    -- )
    AND (
        SERVED_MSISDN in (
            select distinct msisdn from (
                select distinct msisdn,a.imsi,sdate traffic_date,event_date as sales_date
                from (select distinct imsi,event_date from dmc_bi.esim_sales2 
                union all 
                select distinct msisdn imsi,event_date from dmc_bi.esim_sales where event_date between '2022-01-01' and '2022-05-31') a 
                left join (select distinct imsi,imei,msisdn,sdate from mon.spark_ft_imei_online where sdate between '2022-06-01' and '2022-08-31') b on a.imsi = b.imsi
            )
        ) OR OTHER_PARTY in (
            select distinct msisdn from (
                select distinct msisdn,a.imsi,sdate traffic_date,event_date as sales_date
                from (select distinct imsi,event_date from dmc_bi.esim_sales2 
                union all 
                select distinct msisdn imsi,event_date from dmc_bi.esim_sales where event_date between '2022-01-01' and '2022-05-31') a 
                left join (select distinct imsi,imei,msisdn,sdate from mon.spark_ft_imei_online where sdate between '2022-06-01' and '2022-08-31') b on a.imsi = b.imsi
            ) 
        )
    )

    group by TRANSACTION_DATE
    , TRANSACTION_TIME
    , TRANSACTION_DIRECTION
    , substr(TRANSACTION_TYPE, 1, 3)
    , TRANSACTION_SERVICE_CODE
    , SUBSCRIBER_TYPE
    , SERVED_MSISDN
    , OTHER_PARTY
    , PARTNER_GT
    , SUBSTR (MSC_SOURCE_FILE, 1, 7)
    , SERVED_MSISDN
    , OTHER_PARTY

)T1
GROUP BY
     SDATE , substr(HEURE, 1, 2)
    , TRANSACTION_TYPE --USAGE_APPEL
    ,  TYPE_APPEL , TYPE_ABONNE
    --,TYPE_HEURE
    , RECORD_TYPE
    ,  CALLER_SUBR
    ,  PARTNER_ID
    ,   PARTNER_GT
    ,  CRA_SRC
    , TRANSACTION_SERVICE_CODE
    ,  PARTNER_GT_LEN
    ,  PARTNER_ID_LEN
    ,  CALLER_SUBR_LEN
    , ( CASE WHEN PARTNER_ID NOT IN ('BELG', 'FTLD', 'BICS')
        THEN (CASE WHEN SUBSTR (PARTNER_ID_PREFIX, 1, 3)  = '237'
            THEN IF(SUBSTR (PARTNER_ID_PREFIX, 4, 2)="",NULL,SUBSTR (PARTNER_ID_PREFIX, 4, 2))
                ELSE IF(SUBSTR (PARTNER_ID_PREFIX, 1, 2)="",NULL ,SUBSTR (PARTNER_ID_PREFIX, 1, 2))
            END )
        ELSE IF(substr(PARTNER_ID_PREFIX, 1, 9)="",NULL,substr(PARTNER_ID_PREFIX, 1, 9))
        END )
    , ( CASE WHEN PARTNER_GT NOT IN ('BELG', 'FTLD', 'BICS')
            THEN (CASE WHEN SUBSTR (PARTNER_GT_PREFIX, 1, 3)  = '237'
                    THEN IF( SUBSTR (PARTNER_GT_PREFIX, 4, 2)="",NULL, SUBSTR (PARTNER_GT_PREFIX, 4, 2))
                    ELSE IF(SUBSTR (PARTNER_GT_PREFIX, 1, 2)="",NULL,SUBSTR (PARTNER_GT_PREFIX, 1, 2))
                    END )
            ELSE IF(substr(PARTNER_GT_PREFIX, 1, 9)="",NULL,substr(PARTNER_GT_PREFIX, 1, 9))
        END )
    , TRUNCK_OUT, TRUNCK_IN, OLD_CALLED_NUMBER, OLD_CALLING_NUMBER,FN_INTERCO_DESTINATION(SERVICECENTRE)
    , ( CASE WHEN SERVICECENTRE NOT IN ('BELG', 'FTLD', 'BICS')  THEN (CASE WHEN SUBSTR (SERVICECENTRE_PREFIX, 1, 3)  = '237' THEN SUBSTR (SERVICECENTRE_PREFIX, 4, 2) ELSE SUBSTR (SERVICECENTRE_PREFIX, 1, 2) END ) ELSE SERVICECENTRE_PREFIX END )
    , SERVICECENTRE_LEN
    , OPERATOR_CODE
    , SERVED_PARTY_LOCATION
    , SERVED_MSISDN
    , OTHER_PARTY

