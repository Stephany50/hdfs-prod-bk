create table junk.TT_FT_BDI_II3 as
SELECT
   -- A.*,
    A.EVENT_DATE      ,
    A.MSISDN                          ,
    A.TYPE_PIECE                      ,
    A.NUMERO_PIECE                    ,
    A.NOM                             ,
    A.DATE_NAISSANCE                  ,
    A.DATE_EXPIRATION                 ,
    A.ADDRESSE                        ,
    A.NUMERO_PIECE_TUTEUR             ,
    A.NOM_PARENT                      ,
    A.DATE_NAISSANCE_TUTEUR           ,
    A.NOM_STRUCTURE                   ,
    A.NUMERO_REGISTRE_COMMERCE      ,
    A.NUMERO_PIECE_REP_LEGAL          ,
    A.DATE_ACTIVATION                 ,
    A.DATE_CHANGEMENT_STATUT          ,
    A.STATUT_BSCS                     ,
    A.ODBINCOMINGCALLS                ,
    A.ODBOUTGOINGCALLS                ,
    A.IMEI                           ,
    A.STATUT_DEROGATION              ,
    A.REGION_ADMINISTRATIVE           ,
    A.REGION_COMMERCIALE              ,
    A.SITE_NAME                       ,
    A.VILLE                           ,
    A.LONGITUDE                      ,
    A.LATITUDE                        ,
    A.OFFRE_COMMERCIALE               ,
    A.TYPE_CONTRAT                    ,
    A.SEGMENTATION                    ,
    A.STATUT_IN                       ,
    A.NUMERO_PIECE_ABSENT            ,
    A.NUMERO_PIECE_TUT_ABSENT        ,
    A.NUMERO_PIECE_INF_4             ,
    A.NUMERO_PIECE_TUT_INF_4         ,
    A.NUMERO_PIECE_NON_AUTHORISE     ,
    A.NUMERO_PIECE_TUT_NON_AUTH      ,
    A.NUMERO_PIECE_EGALE_MSISDN      ,
    A.NUMERO_PIECE_TUT_EGALE_MSISDN  ,
    A.NUMERO_PIECE_A_CARACT_NON_AUTH ,
    A.NUMERO_PIECE_TUT_CARAC_NON_A   ,
    A.NUMERO_PIECE_UNIQUEMENT_LETTRE ,
    A.NUMERO_PIECE_TUT_UNIQ_LETTRE   ,
    A.NOM_ABSENT                     ,
    A.NOM_PARENT_ABSENT              ,
    A.NOM_DOUTEUX                    ,
    A.NOM_PARENT_DOUTEUX             ,
    A.DATE_NAISSANCE_ABSENT          ,
    A.DATE_NAISSANCE_TUT_ABSENT      ,
    A.DATE_EXPIRATION_ABSENT         ,
    A.ADDRESSE_ABSENT                ,
    A.ADDRESSE_DOUTEUSE              ,
    A.TYPE_PERSONNE_INCONNU          ,
    A.MINEUR_MAL_IDENTIFIE           ,
    A.INSERT_DATE                     ,
    A.ADRESSE_ABSENT                  ,
    A.EST_PREMIUM                     ,
    A.TYPE_PIECE_TUTEUR               ,
    A.ADRESSE_TUTEUR                  ,
    A.ACCEPTATION_CGV                 ,
    A.CONTRAT_SOUCRIPTION             ,
    A.DISPONIBILITE_SCAN              ,
    A.PLAN_LOCALISATION               ,
    A.TYPE_PERSONNE_I                 ,
    (CASE
        WHEN UPPER(A.TYPE_PERSONNE_I) = 'PM' THEN
            (
                CASE
                    WHEN UPPER(A.OFFRE_COMMERCIALE) IN ('POSTPAID DATALIVE', 'POSTPAID GPRSTRACKING', 'POSTPAID SMARTRACK', 'PREPAID DATALIVE') THEN 'MACHINE_2_MACHINE'
                    ELSE 'PERSONNE_MORALE' END
            )
        WHEN UPPER(A.TYPE_PERSONNE_I) = 'PP' AND A.DATE_NAISSANCE IS NOT NULL AND A.DATE_NAISSANCE > ADD_MONTHS(A.EVENT_DATE, -18 * 12) THEN 'MINEUR'
        WHEN UPPER(A.TYPE_PERSONNE_I) = 'PP' AND A.DATE_NAISSANCE IS NOT NULL AND A.DATE_NAISSANCE <= ADD_MONTHS(A.EVENT_DATE, -18 * 12) THEN 'MAJEUR'
        ELSE UPPER(A.TYPE_PERSONNE_I) END) TYPE_PERSONNE,
    --(CASE WHEN H.MSISDN IS NOT NULL THEN H.EVENT_DATE ELSE A.EVENT_DATE END) DATE_ACQUISITION,
    (CASE WHEN A.DATE_NAISSANCE > CURRENT_DATE THEN 'OUI' ELSE 'NON' END) DATE_NAISSANCE_DOUTEUX,
    (CASE WHEN A.DATE_NAISSANCE_TUTEUR > CURRENT_DATE OR EXTRACT(YEAR FROM CURRENT_TIMESTAMP) - EXTRACT(YEAR FROM A.DATE_NAISSANCE_TUTEUR) < 21 THEN 'OUI' ELSE 'NON' END) DATE_NAISSANCE_TUT_DOUTEUX,
    (CASE WHEN A.DATE_EXPIRATION <= A.DATE_ACTIVATION OR A.DATE_EXPIRATION > ADD_MONTHS(CURRENT_DATE, 120) THEN 'OUI' ELSE 'NON' END) DATE_EXPIRATION_DOUTEUSE,
    (CASE WHEN A.DATE_EXPIRATION IS NULL OR  A.DATE_EXPIRATION < '2019-09-22' THEN 'OUI' ELSE 'NON' END) CNI_EXPIRE,
    (CASE WHEN F.NUMERO_PIECE IS NOT NULL AND A.TYPE_PERSONNE_I='PP' THEN 'OUI' ELSE 'NON' END) MULTI_SIM,
    (CASE WHEN D.MSISDN IS NULL THEN 'NON' ELSE 'OUI' END) EST_PRESENT_ZEB,
    (Case when 1 = 1 then (CASE WHEN C.MSISDN IS NULL THEN 'NON' WHEN A.OFFRE_COMMERCIALE LIKE 'POST%' THEN 'OUI' ELSE (case when C.COMGP_STATUS='ACTIF' then 'OUI' when  C.COMGP_STATUS='INACT' then 'NON' else 'UNKNOWN' end)  END) else 'N/A' end)  EST_PRESENT_ART,
    (Case when 1 = 1 then (CASE WHEN C.MSISDN IS NULL THEN 'NON' WHEN A.OFFRE_COMMERCIALE LIKE 'POST%' THEN 'OUI' ELSE (case when C.GP_STATUS='ACTIF' then 'OUI' when  C.GP_STATUS='INACT' then 'NON' else 'UNKNOWN' end)  END) else 'N/A' end) EST_PRESENT_GP,
    (CASE WHEN A.STATUT_IN IN ('ACTIVE', 'INACTIVE') THEN 'OUI' ELSE 'NON' END) EST_PRESENT_OCM, -- Parc Commercial
    (CASE WHEN G.MSISDN IS NULL THEN 'NON' ELSE 'OUI' END) EST_CLIENT_VIP,
    --Insertion Datamart OM
    --(CASE WHEN CURRENT_DATE - B.LAST_ACTIVE_DAY <= 90 THEN 'OUI' ELSE 'NON' END) EST_ACTIF_DATA,
    --Insertion Conso_Month
         A.DATE_VALIDATION_BO              ,
    A.STATUT_VALIDATION_BO            ,
   A.MOTIF_REJET_BO
FROM junk.TT_FT_BDI4 A
LEFT JOIN
(
    SELECT DISTINCT MSISDN, COMGP_STATUS, GP_STATUS
    FROM tmp.FT_ACCOUNT_ACTIVITY
    WHERE EVENT_DATE = date_add('2019-09-22',1)
) C ON A.MSISDN = C.MSISDN
LEFT JOIN
(
    SELECT DISTINCT PRIMARY_MSISDN MSISDN
    FROM CDR.SPARK_IT_ZEBRA_MASTER
    WHERE transaction_date ='2019-09-22' --select max(generated_date) FROM CDR.SPARK_IT_ZEBRA_MASTER --a modifier
) D ON A.MSISDN = D.MSISDN
LEFT JOIN
(
    SELECT NUMERO_PIECE
    FROM junk.TT_FT_BDI4
    WHERE NOT(ODBOUTGOINGCALLS = '1' AND ODBINCOMINGCALLS = '1') -- EST_SUSPENDU
    GROUP BY NUMERO_PIECE
    HAVING COUNT(*) > 3
) F ON A.NUMERO_PIECE = F.NUMERO_PIECE
LEFT JOIN (SELECT DISTINCT MSISDN FROM DIM.DT_BDI_VIP) G ON A.MSISDN = G.MSISDN
--LEFT JOIN (SELECT DISTINCT MSISDN, EVENT_DATE FROM MON.FT_BDI WHERE EVENT_DATE = '2019-09-22' - 1) H ON A.MSISDN = H.MSISDN;