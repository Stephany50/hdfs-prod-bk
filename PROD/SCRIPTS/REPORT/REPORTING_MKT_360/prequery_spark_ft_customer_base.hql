SELECT 
IF(
    T_1.FT_EXIST = 0  and 
    T_2.FT_EXIST > 0  and 
    T_3.FT_EXIST > 0  and 
    T_4.FT_EXIST > 0  and 
    T_5.FT_EXIST > 0  and 
    T_5.FT_EXIST > 0  and 
    T_6.FT_EXIST > 0  and 
    T_7.FT_EXIST > 0  and 
    T_8.FT_EXIST > 0  and 
    T_9.FT_EXIST > 0  and 
    T_10.FT_EXIST > 0  and 
    T_11.FT_EXIST > 0  and 
    T_12.FT_EXIST > 0  and 
    T_13.FT_EXIST > 0  and 
    T_14.FT_EXIST > 0  and 
    T_15.FT_EXIST > 0   
    ,"OK","NOK"
    
) STATUT
FROM
(SELECT COUNT(*) FT_EXIST FROM mon.spark_ft_customer_base WHERE event_date='###SLICE_VALUE###') T_1,
(SELECT COUNT(*) FT_EXIST FROM MON.SPARK_FT_ACCOUNT_ACTIVITY WHERE event_date = date_add('###SLICE_VALUE###', 1)) T_2,
(SELECT COUNT(*) FT_EXIST FROM MON.SPARK_FT_OG_IC_CALL_SNAPSHOT WHERE event_date = date_add('###SLICE_VALUE###', 1)) T_3,
(SELECT COUNT(*) FT_EXIST FROM MON.SPARK_FT_OMNY_ACCOUNT_SNAPSHOT WHERE event_date = '###SLICE_VALUE###') T_4,
(SELECT COUNT(*) FT_EXIST FROM cdr.spark_IT_OMNY_TRANSACTIONS WHERE TRANSFER_DATETIME = '###SLICE_VALUE###') T_5,
(SELECT COUNT(*) FT_EXIST FROM MON.SPARK_FT_IMEI_ONLINE WHERE SDATE='###SLICE_VALUE###') T_6,
(SELECT COUNT(*) FT_EXIST FROM MON.SPARK_FT_CONTRACT_SNAPSHOT WHERE event_date = date_add('###SLICE_VALUE###', 1)) T_7,
(SELECT COUNT(*) FT_EXIST FROM MON.SPARK_FT_BILLED_TRANSACTION_PREPAID WHERE TRANSACTION_DATE = '###SLICE_VALUE###') T_8,
(SELECT COUNT(*) FT_EXIST FROM MON.SPARK_FT_VAS_REVENUE_DETAIL WHERE TRANSACTION_DATE = '###SLICE_VALUE###') T_9,
(SELECT COUNT(*) FT_EXIST FROM MON.SPARK_FT_OVERDRAFT WHERE TRANSACTION_DATE = '###SLICE_VALUE###') T_10,
(SELECT COUNT(*) FT_EXIST FROM MON.SPARK_FT_CREDIT_TRANSFER WHERE REFILL_DATE = '###SLICE_VALUE###') T_11,
(SELECT COUNT(*) FT_EXIST FROM MON.SPARK_FT_DATA_TRANSFER WHERE TRANSACTION_DATE = '###SLICE_VALUE###') T_12,
(SELECT COUNT(*) FT_EXIST FROM MON.SPARK_FT_EMERGENCY_DATA where TRANSACTION_DATE = '###SLICE_VALUE###') T_13,
(SELECT COUNT(*) FT_EXIST FROM MON.SPARK_FT_SUBS_RETAIL_ZEBRA where TRANSACTION_DATE = '###SLICE_VALUE###') T_14,
(SELECT COUNT(*) FT_EXIST FROM mon.SPARK_FT_SUBSCRIPTION where TRANSACTION_DATE = '###SLICE_VALUE###') T_15