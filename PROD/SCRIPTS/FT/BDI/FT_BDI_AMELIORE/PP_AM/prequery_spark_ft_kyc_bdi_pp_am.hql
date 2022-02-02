select
if(A.nbr = 0 and B.nbr > 0 and C.nbr > 0 ,'OK','NOK')
FROM (select count(*) as nbr from MON.SPARK_FT_KYC_BDI_PP_AM where event_date='###SLICE_VALUE###') A
,(select count(*) as nbr from MON.SPARK_FT_KYC_BDI_PP where event_date='###SLICE_VALUE###') B
,(select count(*) as nbr from MON.SPARK_FT_KYC_BDI_FLOTTE_AM where event_date='###SLICE_VALUE###') C