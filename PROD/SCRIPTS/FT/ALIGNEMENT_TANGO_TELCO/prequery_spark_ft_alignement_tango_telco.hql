select
if(A.nbr = 0 and B.nbr >=10 and C.nbr >=10 and D.nbr >= 10 ,'OK','NOK')
FROM (select count(*) as nbr from MON.SPARK_FT_ALIGNEMENT_TANGO_TELCO where event_date='###SLICE_VALUE###') A
,(select count(*) as nbr from MON.SPARK_FT_KYC_BDI_PP where event_date='###SLICE_VALUE###') B
,(select count(*) as nbr from MON.BACKUP_SPARK_DT_BASE_IDENTIFICATION where processing_date='###SLICE_VALUE###') C
,(select count(*) as nbr from CDR.SPARK_IT_NOMAD_CLIENT_DIRECTORY_30J where original_file_date='###SLICE_VALUE###') D
