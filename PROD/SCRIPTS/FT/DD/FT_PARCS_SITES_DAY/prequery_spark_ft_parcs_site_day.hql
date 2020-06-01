SELECT IF(
T_1.FT_EXIST = 0 AND
T_2.CONTRACT_SNAP>0 AND
T_3.CLIENT_LAST_SITE>0 AND
T_4.REFILL > 0 AND
T_5.CREDIT_TRANSFER > 0 AND
T_6.CONSO_MSISDN_DAY > 0 AND
T_7.SUBSCRIPTION > 0 AND
T_8.CRA_GPRS > 0 AND
T_9.ACCOUNT_ACTIVITY > 0
,"OK","NOK") ACT_MONT_EXIST
FROM
(SELECT COUNT(*) FT_EXIST FROM MON.SPARK_FT_PARCS_SITE_DAY WHERE TO_DATE(EVENT_DATE) = DATE_ADD('###SLICE_VALUE###', -1)) T_1,
(SELECT COUNT(*) CONTRACT_SNAP FROM MON.SPARK_FT_CONTRACT_SNAPSHOT WHERE TO_DATE(EVENT_DATE) = '###SLICE_VALUE###') T_2,
(SELECT COUNT(*) CLIENT_LAST_SITE FROM MON.SPARK_FT_CLIENT_LAST_SITE_DAY WHERE TO_DATE(EVENT_DATE) = DATE_ADD('###SLICE_VALUE###', -1)) T_3,
(SELECT COUNT(*) REFILL FROM MON.SPARK_FT_REFILL WHERE TO_DATE(REFILL_DATE) = DATE_ADD('###SLICE_VALUE###', -1)) T_4,
(SELECT COUNT(*) CREDIT_TRANSFER FROM MON.SPARK_FT_CREDIT_TRANSFER WHERE TO_DATE(REFILL_DATE) = DATE_ADD('###SLICE_VALUE###', -1)) T_5,
(SELECT COUNT(*) CONSO_MSISDN_DAY FROM MON.SPARK_FT_CONSO_MSISDN_DAY WHERE TO_DATE(EVENT_DATE) = DATE_ADD('###SLICE_VALUE###', -1)) T_6,
(SELECT COUNT(*) SUBSCRIPTION FROM MON.SPARK_FT_SUBSCRIPTION WHERE TO_DATE(TRANSACTION_DATE) = DATE_ADD('###SLICE_VALUE###', -1)) T_7,
(SELECT COUNT(*) CRA_GPRS FROM MON.SPARK_FT_CRA_GPRS WHERE TO_DATE(SESSION_DATE) = DATE_ADD('###SLICE_VALUE###', -1)) T_8,
(SELECT COUNT(*) ACCOUNT_ACTIVITY FROM MON.SPARK_FT_ACCOUNT_ACTIVITY WHERE TO_DATE(EVENT_DATE) = '###SLICE_VALUE###') T_9
