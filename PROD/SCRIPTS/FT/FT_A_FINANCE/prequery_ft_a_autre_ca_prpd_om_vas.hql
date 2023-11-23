SELECT IF(D.nbr_om_vas =0 and (A.nbr_omny_trans > 0 or C.nbr_zebra_trans > 0) and B.nbr_contract_snap > 0, 'OK', 'NOK') 
FROM
(SELECT COUNT(*) nbr_om_vas FROM AGG.FT_A_AUTRE_CA_PRPD_OM_VAS WHERE jour='###SLICE_VALUE###') D,
(SELECT COUNT(*) nbr_omny_trans FROM CDR.SPARK_IT_OMNY_TRANSACTIONS WHERE transfer_datetime='###SLICE_VALUE###') A,
(SELECT COUNT(*) nbr_contract_snap FROM mon.spark_ft_contract_snapshot WHERE event_date='###SLICE_VALUE###') B,
(SELECT COUNT(*) nbr_zebra_trans FROM CDR.SPARK_IT_ZEBRA_TRANSAC WHERE transfer_date='###SLICE_VALUE###') C