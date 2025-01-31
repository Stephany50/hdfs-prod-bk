SELECT
   DATE_FORMAT(DATE_INSCRIPT, 'dd/MM/yyyy') DATE_INSCRIPT,
   EFF_INSCRIPTEUR EFF_INSCRIPTEUR, 
   NBR_SDT_CONFORME NBR_SDT_CONFORME,
   NBR_INSCRIPTION NBR_INSCRIPTION, 
   NBR_SDT_CONFORME_CUMULE NBR_SDT_CONFORME_CUMULE,
   NBR_INSCRIPTION_CUMULE NBR_INSCRIPTION_CUMULE
FROM MON.SPARK_FT_SDT_OM
WHERE DATE_INSCRIPT = '###SLICE_VALUE###'