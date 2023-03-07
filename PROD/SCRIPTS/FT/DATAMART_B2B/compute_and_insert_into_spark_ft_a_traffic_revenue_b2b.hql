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
R4.horizon_offer_desc OFFER,
sum(nvl(R2.taxed_amount,0)) REVENUE,
R2.transaction_date EVENT_DATE
FROM AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY  R2
LEFT JOIN  DIM.DT_USAGES R1
ON R2.SERVICE_CODE=R1.usage_code
LEFT JOIN DIM.DT_OFFER_PROFILES R4
ON  R4.PROFILE_CODE=upper(R2.COMMERCIAL_OFFER_CODE)
LEFT JOIN DIM.DT_DESTINATIONS R5
ON R5.dest_id = R2.destination

WHERE  R2.TRAFFIC_MEAN='REVENUE' AND R2.source_data not in ('FT_SUBS_RETAIL_ZEBRA','FT_REFILL') AND R2.transaction_date = '###SLICE_VALUE###' and R1.product is not null and R4.horizon_offer_desc is not null 
AND R4.horizon_offer_desc in
("Pro Flex plus",
"FLEX PLUS ",
"Forfait Mix",
"Pro Flex",
"Pro/Access",
"PLUS PLUS",
"FLEX",
"Pro/Access plus",
"SCHLUMBERGER",
"FLEX PLUS",
"Forfait data",
"FLEX  PLUS",
"FLEX PLUS 5K UN",
"ORANGE COMMUNAUTE SOHO",
"BUNDLE PLUS",
"MIX",
"PLATINUM ",
"ONE PLUS",
"SOHO PLUS",
"COMMUNITY PLUS",
"PRO PLUS",
"Flex Plus 20K UN",
"Flex Plus 10K UN",
"CLOUD",
"MOBILITY",
"Flex Plus 30K UN",
"MAGIC PLUS",
"Offre data",
"CLOUD INTENSE",
"Orange Pro L",
"ATHOS",
"HONORABLE PLUS",
"Orange Pro M",
"DATA LIVE MIX BUNDLE L1",
"DATA LIVE 5MO",
"DATA LIVE MIX SMARTRACK",
"CLOUD MOIS ++")

GROUP BY
  R1.PRODUCT,  
  R1.USAGE_DESCRIPTION, 
  R4.horizon_offer_desc,
  R2.transaction_date
) RF

 
