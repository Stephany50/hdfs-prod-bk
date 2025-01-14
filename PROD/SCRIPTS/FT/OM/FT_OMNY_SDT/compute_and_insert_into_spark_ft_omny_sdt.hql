INSERT INTO MON.SPARK_FT_OMNY_SDT
SELECT
    SD.MSISDN
    , SD.USER_NAME
    , SD.ADDRESS
    , IF(SD.MSISDN IS NULL OR NOMAD.TELEPHONE IS NULL OR SD.INSCRIPTEUR IS NOT NULL, SD.INSCRIPTEUR, NOMAD.LOGINVENDEUR) INSCRIPTEUR
    , SD.ID_NUMBER
    , SD.SEX
    , SD.DATE_OF_BIRTH
    , SD.MIDATEDEP
    , SD.MTT_DEPOT
    , SD.NB_DEPOT
    , SD.FIRSTDEPOT
    , PURT.FIRSTTRAN
    , PURT.DEST_TRAN
    , PURT.SERV_TRAN
    , PURT.MIDATETR
    , PURT.MTT_TRANS
    , PURT.NB_TRANSACTION
    , (CASE WHEN PURT.SENDER_MSISDN IS NOT NULL THEN 'SDT' ELSE SD.REG_LEVEL END) REG_LEVEL
    , (CASE WHEN SD.FIRSTDEPOT>=500 THEN 'VRAI' ELSE 'FAUX' END) SEUIL_OK
    , (CASE WHEN SD.MSISDN = SD.INSCRIPTEUR THEN 'VRAI' ELSE 'FAUX' END) AUTO_INSCRIPTION
    , (CASE WHEN PURT.SERV_TRAN = 'P2P' AND SD.INSCRIPTEUR = PURT.DEST_TRAN THEN 'VRAI' ELSE 'FAUX' END) BOUCLE_P2P
    , (CASE WHEN DB.MSISDN IS NOT NULL THEN 'VRAI' ELSE 'FAUX' END) DOUBLE_PROFIL
    , 'FAUX' BOUCLE
    , (CASE WHEN LENGTH(SD.ID_NUMBER) IN ('9','10') AND SUBSTR(SD.ID_NUMBER,1,3)<'130' THEN 'VRAI' ELSE 'FAUX' END) CONFORMITE
    , CURRENT_TIMESTAMP() INSERT_DATE
    , SD.DATE_INSCRIPT
FROM
TMP.TT_OMNY_SDT_1 SD
LEFT JOIN 
TMP.TT_OMNY_SDT_2 PURT ON SD.MSISDN = PURT.SENDER_MSISDN
LEFT JOIN
TMP.TT_OMNY_SDT_3 DB ON SD.MSISDN = DB.MSISDN
LEFT JOIN
TMP.TT_OMNY_SDT_4 NOMAD ON SD.MSISDN = NOMAD.TELEPHONE