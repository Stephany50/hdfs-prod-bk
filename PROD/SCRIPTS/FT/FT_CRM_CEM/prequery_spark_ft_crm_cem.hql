select
if(A.otarie>=10 and B.ftcltd >= 10 and B2.ftclast_td >= 10 and C.ftcsnsht >= 10  and C2.ftimon >= 10 and D.ftcrmcem  = 0
            ,'OK','NOK')
FROM (select count(*) as otarie from MON.SPARK_FT_OTARIE_DATA_TRAFFIC_DAY where TRANSACTION_DATE='###SLICE_VALUE###') A
,(select count(*) as ftcltd from MON.SPARK_FT_CLIENT_LAC_TRAFFIC_DAY  where event_date='###SLICE_VALUE###') B
,(select count(*) as ftclast_td from MON.SPARK_FT_CLIENT_LAST_SITE_DAY  where event_date='###SLICE_VALUE###') B2
,(SELECT count(*) as ftcsnsht from MON.SPARK_FT_CONTRACT_SNAPSHOT where EVENT_DATE= date_add('###SLICE_VALUE###', 1)) C
,(SELECT count(*) as ftimon FROM MON.SPARK_FT_IMEI_ONLINE where SDATE = '###SLICE_VALUE###') C2
,(SELECT count(*) as ftcrmcem FROM MON.SPARK_FT_CRM_CEM where event_date='###SLICE_VALUE###') D