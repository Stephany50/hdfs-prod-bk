INSERT INTO  MON.SPARK_FT_ACTIVATION_ALL_BY_CUMUL
SELECT
   TO_DATE(a.COMPLETED_DATE) ACTIVATION_DATE
   ,b.IDENTIFICATEUR
   ,b.Genre
   ,b.Civilite
   ,b.EST_SNAPPE
   ,b.Flux_source
   ,PRICE_PLAN_NAME
   ,CUSTOMER_TYPE
   ,CONTRACT_TYPE
   ,CUSTOMER_PROFILE
   ,OFFER_NAME
   ,SEGMENTATION
   ,count(a.ACC_NBR) NBR_MSISDN
   , current_timestamp INSERT_DATE
   , "###SLICE_VALUE###" EVENT_DATE
   from
(SELECT DISTINCT
 ACC_NBR.ACC_NBR
,ACC_NBR.ACC_NBR_STATE
,ACC_NBR.STATE_DATE
,PROD.CREATED_DATE PROD_CREATED_DATE
,PROD.BLOCK_REASON
,PROD.COMPLETED_DATE
,upper(PRICE_PLAN.PRICE_PLAN_NAME) PRICE_PLAN_NAME
,upper(PROFIL.CUSTOMER_TYPE) CUSTOMER_TYPE
,upper(PROFIL.CONTRACT_TYPE) CONTRACT_TYPE
,upper(PROFIL.CUSTOMER_PROFILE) CUSTOMER_PROFILE
,upper(PROFIL.OFFER_NAME) OFFER_NAME
,SEGMENTATION
FROM
CDR.SPARK_IT_ZTE_ACC_NBR_EXTRACT ACC_NBR
LEFT JOIN
CDR.SPARK_IT_ZTE_SUBS_EXTRACT SUBS
ON ACC_NBR.ACC_NBR=SUBS.ACC_NBR
LEFT JOIN
CDR.SPARK_IT_ZTE_PROD_EXTRACT PROD
ON SUBS.SUBS_ID=PROD.PROD_ID
LEFT JOIN
CDR.SPARK_IT_ZTE_PRICE_PLAN_EXTRACT PRICE_PLAN
ON SUBS.PRICE_PLAN_ID=PRICE_PLAN.PRICE_PLAN_ID
LEFT JOIN
dim.dt_offer_profiles PROFIL
ON upper(PRICE_PLAN.PRICE_PLAN_NAME) = upper(PROFIL.PROFILE_CODE)
WHERE   (PROD.PROD_STATE in ('A','D') OR ((PROD.PROD_STATE='E' AND SUBSTR(PROD.BLOCK_REASON, 3, 1) <> '0') or (PROD.PROD_STATE='E' AND PROD.BLOCK_REASON='20000000000000')))
            AND ACC_NBR.ORIGINAL_FILE_DATE =  DATE_ADD("###SLICE_VALUE###",1)
            AND SUBS.ORIGINAL_FILE_DATE =  DATE_ADD("###SLICE_VALUE###",1)
            AND PROD.ORIGINAL_FILE_DATE = DATE_ADD("###SLICE_VALUE###",1)
            AND PROD.COMPLETED_DATE BETWEEN TO_DATE((substr("###SLICE_VALUE###",1,7)) || '-01') and "###SLICE_VALUE###"
            AND PROD.INDEP_PROD_ID IS NULL --Exclure les lignes utilisées pour le fonctionnement interne de l'IN -MAJ le 15/06/2013 pour effet a partir du 01/06/2013
            AND NVL(PROD.ROUTING_ID,'100') IN ('1','2','3') --Se limiter au partition (1,2,3)
            ) a
           left join
           (select MSISDN,DATE_IDENTIFICATION,IDENTIFICATEUR,FICHIER_CHARGEMENT Flux_source
          ,case when Civilite='1' then 'Monsieur(Mr)' when Civilite='2' then 'Madame(Mme)' else Civilite  end Civilite
          ,Genre
          ,EST_SNAPPE
          from DIM.DT_BASE_IDENTIFICATION ) b
          ON a.ACC_NBR = b.MSISDN
          group by
          TO_DATE(a.COMPLETED_DATE)
          ,b.IDENTIFICATEUR
          ,b.Genre
          ,b.Civilite
          ,b.EST_SNAPPE
          ,b.Flux_source
          ,PRICE_PLAN_NAME
          ,PRICE_PLAN_NAME
          ,PRICE_PLAN_NAME
          ,CUSTOMER_TYPE
          ,CONTRACT_TYPE
          ,CUSTOMER_PROFILE
          ,OFFER_NAME
          ,SEGMENTATION