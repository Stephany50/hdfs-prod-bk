SELECT IF(
T_1.FT_EXIST = 0 AND
T_2.POST_CONTRACT_SNAP>0
,"OK","NOK") ACT_MONT_EXIST
FROM
(SELECT COUNT(*) FT_EXIST FROM MON.SPARK_FT_ACTIVATION_MONTH WHERE ACTIVATION_DATE = TO_DATE('###SLICE_VALUE###')) T_1,
(SELECT COUNT(*) POST_CONTRACT_SNAP FROM MON.SPARK_FT_CONTRACT_SNAPSHOT WHERE EVENT_DATE = DATE_ADD(TO_DATE('###SLICE_VALUE###'), 1)) T_2