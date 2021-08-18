--Cette table est liée a la table MON.SPARK_FT_BDI_PERS_MORALE_DETAIL, raison pour laquelle, elles seront calculées ensemble.
select
if(A.nbr = 0 and B.nbr = 0 and C.nbr > 0 ,'OK','NOK') FROM
(select count(*) as nbr from AGG.SPARK_FT_A_BDI_PERS_MORALE where event_date='###SLICE_VALUE###') A
,(select count(*) as nbr from MON.SPARK_FT_BDI_PERS_MORALE_DETAIL where event_date='###SLICE_VALUE###') B
,(select count(*) as nbr from MON.SPARK_FT_BDI_PERS_MORALE where event_date='###SLICE_VALUE###') C