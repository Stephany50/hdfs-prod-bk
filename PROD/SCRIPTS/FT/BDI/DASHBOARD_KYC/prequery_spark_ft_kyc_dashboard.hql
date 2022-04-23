select
if(A.nbr = 0 and B.nbr = 0 and C.nbr = 0 and D.nbr = 0 and E.nbr > 0 and F.nbr > 0 and G.nbr > 0 and H.nbr > 0 and I.nbr > 0,'OK','NOK')   FROM 
 (select count(*) as nbr from AGG.SPARK_FT_A_KYC_DASHBOARD where event_date='###SLICE_VALUE###') A
,(select count(*) as nbr from AGG.SPARK_FT_A_KYC_DASHBOARD_DELTA where event_date='###SLICE_VALUE###') B
,(select count(*) as nbr from AGG.SPARK_FT_A_KYC_DASHBOARD_DETAILS where event_date='###SLICE_VALUE###') C 
,(select count(*) as nbr from AGG.SPARK_FT_A_KYC_DASHBOARD_KPIS_TELCO where event_date='###SLICE_VALUE###') D 
,(select count(*) as nbr from MON.SPARK_FT_ALIGNEMENT_TANGO_TELCO where event_date='###SLICE_VALUE###') E
,(select count(*) as nbr from AGG.SPARK_FT_A_BDI where event_date='###SLICE_VALUE###') F
,(select count(*) as nbr from AGG.SPARK_FT_A_BDI_B2B where event_date='###SLICE_VALUE###') G
,(select count(*) as nbr from MON.SPARK_FT_ABONNE_HLR where event_date='###SLICE_VALUE###') H
,(select count(*) as nbr from MON.SPARK_FT_KYC_ZSMART where event_date='###SLICE_VALUE###') I
