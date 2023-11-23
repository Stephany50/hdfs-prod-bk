SELECT IF(D.nbr_prpd_eqt =0 and A.nbr_eqt_prpd > 0 and B.nbr_contract_snap > 0, 'OK', 'NOK') 
FROM
(SELECT COUNT(*) nbr_prpd_eqt FROM AGG.SPARK_FT_A_BILAN_CDR_PRPD_EQT WHERE jour='###SLICE_VALUE###') D,
(SELECT COUNT(*) nbr_eqt_prpd FROM MON.SPARK_FT_EDR_PRPD_EQT WHERE event_date='###SLICE_VALUE###') A,
(SELECT COUNT(*) nbr_contract_snap FROM mon.spark_ft_contract_snapshot WHERE event_date='###SLICE_VALUE###') B