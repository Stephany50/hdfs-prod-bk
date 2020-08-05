select
if(A.otarie>=10 and B.ftcltd >= 10 and C.ftcsnsht >= 10 and D.ftcrmcem  = 0
            ,'OK','NOK')
FROM (select count(*) as otarie from MON.SPARK_FT_OTARIE_DATA_TRAFFIC_DAY where TRANSACTION_DATE='###SLICE_VALUE###') A
,(select count(*) as ftcltd from MON.SPARK_FT_CLIENT_LAC_TRAFFIC_DAY  where event_date='###SLICE_VALUE###') B
,(SELECT count(*) as ftcsnsht FROM MON.SPARK_FT_CONTRACT_SNAPSHOT where event_date='###SLICE_VALUE###') C
,(SELECT count(*) as ftcrmcem FROM MON.SPARK_FT_CRM_CEM where event_date='###SLICE_VALUE###') D