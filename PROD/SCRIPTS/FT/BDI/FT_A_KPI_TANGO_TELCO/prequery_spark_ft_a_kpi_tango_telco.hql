SELECT IF(A.KPI_TANGO_TELCO = 0 AND B.BASE >0 ,"OK","NOK") FROM
(SELECT COUNT(*) KPI_TANGO_TELCO FROM AGG.SPARK_FT_A_KPI_TANGO_TELCO WHERE EVENT_DATE= '###SLICE_VALUE###') A,
(SELECT COUNT(*) BASE FROM MON.SPARK_FT_ALIGNEMENT_TANGO_TELCO WHERE EVENT_DATE= '###SLICE_VALUE###') B
