select
if(A.nbr = 0 and B.nbr > 0 ,'OK','NOK') FROM
(select count(*) as nbr from AGG.SPARK_FT_A_BDI_PERS_MORALE where event_date='###SLICE_VALUE###') A
,(select count(*) as nbr from MON.SPARK_FT_KYC_BDI_FLOTTE where event_date='###SLICE_VALUE###') B ----- Permet d'obtenir toutes les personnes morales actives