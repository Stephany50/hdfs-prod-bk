select
if(A.bdi > 0 and B.hlr > 0 and C.zsmart > 0 ,'OK','NOK')
FROM (select count(*) as bdi from MON.SPARK_FT_BDI where event_date='###SLICE_VALUE###') A
,(select count(*) as hlr from MON.SPARK_FT_ABONNE_HLR where event_date='###SLICE_VALUE###') B
,(select count(*) as zsmart from MON.SPARK_FT_ZSMART_CONF where event_date='###SLICE_VALUE###') C
