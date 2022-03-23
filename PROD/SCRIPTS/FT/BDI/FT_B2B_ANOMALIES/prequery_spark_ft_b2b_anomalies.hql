select
if(A.nbr = 0 and B.nbr >0 and C.nbr >0 ,'OK','NOK') FROM
(select count(*) as nbr from MON.SPARK_FT_B2B_ANOMALIES where event_date='###SLICE_VALUE###') A
,(select count(*) as nbr from MON.SPARK_FT_KYC_BDI_FLOTTE where event_date='###SLICE_VALUE###') B
,(select count(*) as nbr from MON.SPARK_FT_KYC_CRM_B2B where event_date='###SLICE_VALUE###') C