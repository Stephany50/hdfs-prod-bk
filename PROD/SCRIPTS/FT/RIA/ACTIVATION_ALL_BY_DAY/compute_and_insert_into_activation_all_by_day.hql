INSERT INTO MON.ACTIVATION_ALL_BY_DAY
   SELECT
   a.EVENT_DATE,
   a.ACC_NBR MSISDN,
   a.COMPLETED_DATE ACTIVATION_DATE,
   b.IDENTIFICATEUR,
   b.Genre,
   b.Civilite,
   b.EST_SNAPPE,
   SYSDATE
   from
    (SELECT DISTINCT
         TO_DATE(S_SLICE_VALUE,'yyyymmdd') EVENT_DATE,
         ACC_NBR.ACC_NBR,
         ACC_NBR.ACC_NBR_STATE,
         ACC_NBR.STATE_DATE,
         PROD.CREATED_DATE PROD_CREATED_DATE,
         PROD.BLOCK_REASON,
         PROD.COMPLETED_DATE
    FROM CDR.IT_ZTE_ACC_NBR_EXTRACT ACC_NBR,
         CDR.IT_ZTE_PROD_EXTRACT PROD,
         CDR.IT_ZTE_SUBS_EXTRACT SUBS
    WHERE   (PROD.PROD_STATE in ('A','D') OR ((PROD.PROD_STATE='E' AND SUBSTR(PROD.BLOCK_REASON, 3, 1) <> '0') or (PROD.PROD_STATE='E' AND PROD.BLOCK_REASON='20000000000000')))
            AND ACC_NBR.ORIGINAL_FILE_DATE=TO_DATE(S_NEXT_DATE,'yyyymmdd')
            AND SUBS.ORIGINAL_FILE_DATE=TO_DATE(S_NEXT_DATE,'yyyymmdd')
            AND PROD.ORIGINAL_FILE_DATE=TO_DATE(S_NEXT_DATE,'yyyymmdd')
            AND PROD.COMPLETED_DATE BETWEEN TO_DATE(S_SLICE_VALUE ||' 000000','yyyymmdd hh24miss') and TO_DATE(S_SLICE_VALUE ||' 235959','yyyymmdd hh24miss')
            AND ACC_NBR.ACC_NBR=SUBS.ACC_NBR(+)
            AND SUBS.SUBS_ID=PROD.PROD_ID(+)
            AND PROD.INDEP_PROD_ID IS NULL --Exclure les lignes utilisées pour le fonctionnement interne de l'IN -MAJ le 15/06/2013 pour effet a partir du 01/06/2013
            AND NVL(PROD.ROUTING_ID,'100') IN ('1','2','3') --Se limiter au partition (1,2,3)
            ) a
   left join
       (   select MSISDN,DATE_IDENTIFICATION,IDENTIFICATEUR,FICHIER_CHARGEMENT,
           case when Civilite='1' then 'Monsieur(Mr)' when Civilite='2' then 'Madame(Mme)' else Civilite  end Civilite,
           Genre,
           EST_SNAPPE
          from DIM.DT_BASE_IDENTIFICATION ) b
   ON a.ACC_NBR = b.MSISDN;