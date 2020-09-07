SELECT IF (ABS(AMOUNT - T.N_TOTAL) <100, 'OK', 'NOK')
FROM
(
    SELECT ROUND(SUM(AMOUNT)*2) AMOUNT
    from
    (
        SELECT SUM(NVL (TRANSACTION_AMOUNT, 0)) AMOUNT
        FROM CDR.SPARK_IT_OMNY_TRANSACTIONS
        WHERE (TRANSFER_STATUS='TS' OR (TRANSFER_STATUS='TF'AND  RECONCILIATION_BY IS NOT NULL) OR (TRANSFER_STATUS='TF'AND SENDER_PRE_BAL<>SENDER_POST_BAL))
            AND TRANSFER_DATETIME ='###SLICE_VALUE###'
        UNION
        SELECT SUM(NVL (COMMISSION_AMOUNT, 0)*3) AMOUNT
        FROM CDR.SPARK_IT_OMNY_COMMISSION_DETAILS
        WHERE COMMISSION_AMOUNT>0
            AND TRANSACTION_DATE ='###SLICE_VALUE###'
        UNION
        SELECT SUM(NVL (SERVICE_CHARGE_AMOUNT, 0)*4) AMOUNT
        FROM CDR.SPARK_IT_OMNY_SERVICES_CHARGES_DETAILS
        WHERE SERVICE_CHARGE_AMOUNT>0
            AND TRANSACTION_DATE ='###SLICE_VALUE###'
    ) M
) R , 
(
    SELECT
        ABS(ROUND(SUM(
        (CASE WHEN LIB LIKE '%cmms%' THEN NVL(MON, 0) ELSE 0 END)*3 -- CMMS 
        + (CASE WHEN LIB LIKE '%chrg%' THEN NVL(MON, 0) ELSE 0 END)*4  -- CHRG
        + (CASE WHEN LIB LIKE '%amnt%' THEN NVL(MON, 0) ELSE 0 END) --  MON
        ))) N_TOTAL
    FROM 
    (select * from MON.SPARK_TTVMW_OM_BICEC_TRANS where EVENT_DATE='###SLICE_VALUE###') T0
) T
