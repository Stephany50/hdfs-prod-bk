SELECT IF(T1.KPI_IS_LOAD=0 AND OM_MARK>0 -- AND 
-- ABS((revenu_frais_valeur_j_0 / revenu_frais_valeur_j_1) - 1) < 0.5 AND 
-- ABS((val_valeur_j_0 / val_valeur_j_1) - 1) < 0.5
,'OK','NOK')
 FROM
(SELECT COUNT(*) KPI_IS_LOAD FROM AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_DG_NEW  WHERE TRANSACTION_DATE='###SLICE_VALUE###' AND JOB_NAME='COMPUTE_KPI_OM_REVENUE')T1,
(SELECT COUNT(*) OM_MARK, SUM(NVL(REVENU, 0)+NVL(FRAIS, 0)) revenu_frais_valeur_j_0, SUM(NVL(VAL, 0)) val_valeur_j_0 FROM AGG.SPARK_REGIONAL_DOM_DASHBOARD WHERE jour ='###SLICE_VALUE###')T2,
(SELECT SUM(NVL(REVENU, 0)+NVL(FRAIS, 0)) revenu_frais_valeur_j_1, SUM(NVL(VAL, 0)) val_valeur_j_1 FROM AGG.SPARK_REGIONAL_DOM_DASHBOARD WHERE jour =date_sub('###SLICE_VALUE###', 1))T3