select
if(A.nbr = 0 and B.nbr >0 and C.nbr >0 ,'OK','NOK') FROM
(select count(*) as nbr from MON.SPARK_FT_KYC_CRM_B2B where event_date='###SLICE_VALUE###') A
,(select count(*) as nbr from MON.SPARK_FT_KYC_CRM_B2B where event_date=DATE_SUB('###SLICE_VALUE###',1)) B
,(select count(*) as nbr from CDR.SPARK_IT_KYC_CRM_B2B where original_file_date=DATE_ADD('###SLICE_VALUE###',1)) C