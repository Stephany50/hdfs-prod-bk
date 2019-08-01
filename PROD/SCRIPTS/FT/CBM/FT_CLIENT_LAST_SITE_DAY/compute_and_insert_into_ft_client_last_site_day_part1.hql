
--add jar hdfs:///PROD/UDF/hive-udf-1.0.1.jar;
--create temporary function FN_GET_OPERATOR_CODE as 'cm.orange.bigdata.udf.GetOperatorCode';
--create temporary function fn_format_msisdn_to_9digits as 'cm.orange.bigdata.udf.GetNnpMsisdn9Digits';

INSERT INTO MON.TT_CLIENT_LAST_SITE_DAY
        SELECT
        fn_format_msisdn_to_9digits(MSISDN) MSISDN,
        SITE_NAME,
        TOWNNAME,
        ADMINISTRATIVE_REGION,
        COMMERCIAL_REGION,
        LAST_LOCATION_DAY,
        OPERATOR_CODE,
        INSERT_DATE,
        '###SLICE_VALUE###'
        FROM MON.FT_CLIENT_LAST_SITE_DAY
        WHERE (OPERATOR_CODE <> 'UNKNOWN_OPERATOR' OR (OPERATOR_CODE = 'UNKNOWN_OPERATOR' AND LAST_LOCATION_DAY >  date_sub('###SLICE_VALUE###', 179))) AND EVENT_DATE = date_sub('###SLICE_VALUE###', 1) ;

