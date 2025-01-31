SELECT IF(A.FT_EXIST = 0 and B.IT_COUNT > 0 and C.IT_COUNT>= 30 and D.IT_COUNT>= 30 and E.IT_COUNT>= 30 , "OK", "NOK") FROM
(SELECT COUNT(*) FT_EXIST FROM AGG.SPARK_KPI_RECO_CREANCES WHERE EVENT_MONTH = '###SLICE_VALUE###') A,
(SELECT COUNT(distinct ORDER_DATE) IT_COUNT FROM CDR.SPARK_IT_CHIFFRE_AFFAIRE where ORDER_DATE = TO_DATE(CONCAT('###SLICE_VALUE###','-01'))) B,
(SELECT COUNT(distinct DATE_SAISIE ) IT_COUNT FROM CDR.SPARK_IT_RAPPORT_DAILY where DATE_SAISIE BETWEEN TO_DATE(CONCAT('###SLICE_VALUE###','-01')) AND LAST_DAY(CONCAT('###SLICE_VALUE###','-01'))) C,
(SELECT COUNT(distinct AS_OF_DATE) IT_COUNT FROM CDR.SPARK_IT_BALANCE_AGEE where AS_OF_DATE BETWEEN TO_DATE(CONCAT('###SLICE_VALUE###','-01')) AND LAST_DAY(CONCAT('###SLICE_VALUE###','-01'))) D,
(SELECT COUNT(distinct DATE_SUSPENSION) IT_COUNT FROM CDR.SPARK_IT_SUSPENSION_DAILY where DATE_SUSPENSION BETWEEN TO_DATE(CONCAT('###SLICE_VALUE###','-01')) AND LAST_DAY(CONCAT('###SLICE_VALUE###','-01'))) E