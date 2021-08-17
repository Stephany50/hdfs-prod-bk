select
if(A.nbr = 0 and B.nbr >0 ,'OK','NOK') FROM
(select count(*) as nbr from MON.SPARK_FT_BDI_PERS_MORALE where event_date='###SLICE_VALUE###') A
,(select count(*) as nbr from CDR.SPARK_IT_BDI_PERS_MORALE where original_file_date=date_add('###SLICE_VALUE###',1)) B