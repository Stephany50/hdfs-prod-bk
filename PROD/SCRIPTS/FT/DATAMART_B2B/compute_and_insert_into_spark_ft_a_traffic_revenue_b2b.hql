INSERT INTO AGG.SPARK_TRAFFIC_REVENUE_B2B

SELECT 
PRODUCT,
PRODUCT_DESCRIPTION,
PROFIL,
MAIN_PRODUCT,
OFFER,
REVENUE,
EVENT_DATE
FROM 
(SELECT
R1.product PRODUCT,
(CASE
    WHEN trim(R4.horizon_offer_desc) = trim('Pro/Access') THEN 'PREPAID'
    ELSE 'HYBRID' 
END) PROFIL,
(CASE
    WHEN R1.usage_description like '%Voix%' THEN 'VOIX'
    WHEN R1.usage_description = 'Parrainage' THEN 'VOIX'
    WHEN R1.usage_description like '%SMS%' THEN 'SMS'
    WHEN R1.usage_description like '%Data%' THEN 'DATA'
    WHEN R1.usage_description like '%DATA%' THEN 'DATA'
    WHEN trim(R1.usage_description) = trim('Frais Transfert P2P') THEN 'Autres'
    WHEN trim(R1.usage_description) = trim('Credit Compte Desactive') THEN 'Autres'
    WHEN trim(R1.usage_description) = trim('Produit indéterminé') THEN 'Autres'
    ELSE 'VAS' 
END) MAIN_PRODUCT,
R1.usage_description PRODUCT_DESCRIPTION,
nvl(R4.horizon_offer_desc,COMMERCIAL_OFFER_CODE) OFFER,
sum(nvl(R2.taxed_amount,0)) REVENUE,
R2.transaction_date EVENT_DATE
FROM  TT.DIM_DT_USAGE_NEW  R1
INNER JOIN  AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY R2
ON R2.SERVICE_CODE=R1.usage_code
INNER JOIN (SELECT * FROM TT.DIM_DT_PROFILE_NEW WHERE SEGMENTATION In ('Staff','B2B','B2C'))R4
ON  R4.PROFILE_CODE=upper(R2.COMMERCIAL_OFFER_CODE)
LEFT JOIN DIM.DT_DESTINATIONS R5
ON R5.dest_id = R2.destination
LEFT JOIN (SELECT * FROM  DIM.DT_DATES  WHERE DATECODE = '###SLICE_VALUE###')R8
ON R2.transaction_date = R8.DATECODE

WHERE  R2.TRAFFIC_MEAN='REVENUE' 
AND R2.OPERATOR_CODE in ('OCM') 
AND R2.sub_account in ('MAIN')
AND R2.transaction_date = '###SLICE_VALUE###' 

GROUP BY
  R1.DOMAIN,
  R1.PRODUCT,  
  R2.sub_account,
  R8.yyyymmdd,
  R8.yyyy,
  R4.CONTRACT_TYPE,
  nvl(R4.horizon_offer_desc,COMMERCIAL_OFFER_CODE),
  R1.USAGE_DESCRIPTION, 
  R4.horizon_offer_desc,
  R2.transaction_date
) RF

 
