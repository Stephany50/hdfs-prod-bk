select
if(A.nbr = 0 and B.nbr > 0 and C.nbr > 0 and D.nbr > 0,'OK','NOK')
FROM (select count(*) as nbr from MON.SPARK_FT_KYC_BDI_FLOTTE_AM where event_date='###SLICE_VALUE###') A
,(select count(*) as nbr from MON.SPARK_FT_KYC_BDI_PP where event_date='###SLICE_VALUE###') B
,(select count(*) as nbr from CDR.SPARK_IT_KYC_BDI_FULL where original_file_date=date_add('###SLICE_VALUE###',1)) C
,(select count(*) as nbr from MON.SPARK_FT_KYC_BDI_FLOTTE where event_date='###SLICE_VALUE###') D

