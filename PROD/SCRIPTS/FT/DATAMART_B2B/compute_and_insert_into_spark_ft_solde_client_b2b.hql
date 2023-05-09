INSERT INTO MON.SPARK_SOLDE_CLIENT_B2B
SELECT
MSISDN,
STD_CODE,
PROFILE,
USAGE,
( CASE 
    WHEN USAGE like '%Compte%' THEN 'U'
    WHEN USAGE like '%Local%' THEN 'U'
    WHEN USAGE like '%International%' THEN 'Min'
    WHEN USAGE like '%Data%' THEN 'Go'
    WHEN USAGE like '%SMS%' THEN 'Nb'
    WHEN USAGE like '%Roaming%' THEN 'Min'
ELSE 'Autres' END ) UNITE,
( CASE 
    WHEN USAGE like '%Compte%' THEN SOLDE/100
    WHEN USAGE like '%Local%' THEN SOLDE/100 
    WHEN USAGE like '%International%' THEN SOLDE/60
    WHEN USAGE like '%Data%' THEN SOLDE/(1024*1024*1024)
    WHEN USAGE like '%SMS%' THEN SOLDE
    WHEN USAGE like '%Roaming%' THEN SOLDE/60
ELSE SOLDE END ) SOLDE,
EVENT_DATE
FROM
   ( select
        subs.acc_nbr MSISDN,
        B.commercial_offer PROFILE,
        R.acct_res_id STD_CODE,
        -(nvl(R.GROSS_BAL, 0) + nvl(R.RESERVE_BAL, 0) + nvl(R.CONSUME_BAL, 0)) SOLDE,
        ( CASE 
            WHEN R.acct_res_id = '1' THEN 'Compte principal'
            WHEN R.acct_res_id IN ('50','75','37','35','59','62','69','91','206','218','222','259','1234','1434') THEN 'Data'
            WHEN R.acct_res_id IN ('183','61','73','58','201','202') THEN 'SMS'
            WHEN R.acct_res_id IN ('187','253','60','195','200') THEN 'Voix Local'
            WHEN R.acct_res_id IN ('131','2534','89','165','242') THEN 'Voix Onnet'
            WHEN R.acct_res_id IN ('64','74') THEN 'Voix International'
            WHEN R.acct_res_id = '78' THEN 'Voix Roaming'
        ELSE 'Autres' END ) USAGE,
        subs.ORIGINAL_FILE_DATE EVENT_DATE
    from 
    (
        select distinct acc_nbr, 
            acct_id, 
            original_file_date 
        from CDR.SPARK_IT_ZTE_SUBS_EXTRACT 
        where original_file_date = '###SLICE_VALUE###'
    ) subs
    INNER JOIN 
    (SELECT main_credit,commercial_offer,access_key FROM MON.SPARK_FT_CONTRACT_SNAPSHOT 
    WHERE EVENT_DATE='###SLICE_VALUE###' AND  upper(commercial_offer) IN 
    ("ARAMIS",
    "ATHOS",
    "CLOUD BEW20K",
    "CLOUD GOLD",
    "CLOUD MAX",
    "CLOUD MOIS",
    "CLOUD MOIS +",
    "CLOUD MOIS ++",
    "CUG",
    "DATA LIVE 10MO",
    "DATA LIVE 13MO",
    "DATA LIVE 30MO",
    "DATA LIVE 3MO",
    "DATA LIVE 5MO",
    "DATA LIVE 7MO",
    "DATA LIVE ILLIMITÉ WITHOUT SMS",
    "DATA LIVE MIX 10MO IHS",
    "DATA LIVE MIX BUNDLE L1",
    "DATA LIVE MIX BUNDLE L2",
    "DATA LIVE MIX BUNDLE L3",
    "DATA LIVE MIX BUNDLE L4",
    "DATA LIVE MIX BUNDLE L5",
    "DATA LIVE MIX BUNDLE L6",
    "DATA LIVE MIX SMARTRACK",
    "ENTREPRENEUR MEDIUM",
    "FLEX 100K",
    "FLEX 10K",
    "FLEX 125K",
    "FLEX 12K",
    "FLEX 150K",
    "FLEX 15K",
    "FLEX 16.5K",
    "FLEX 20K",
    "FLEX 22K",
    "FLEX 250K",
    "FLEX 25K",
    "FLEX 27.5K",
    "FLEX 30K",
    "FLEX 35K",
    "FLEX 38.5K",
    "FLEX 3K",
    "FLEX 40K",
    "FLEX 45K",
    "FLEX 50K",
    "FLEX 55K",
    "FLEX 5K",
    "FLEX 60K",
    "FLEX 65K",
    "FLEX 70K",
    "FLEX 75K",
    "FLEX 80K",
    "FLEX 90K",
    "FLEX FOR QUOTE TEST",
    "FLEX PLUS 100K",
    "FLEX PLUS 10K",
    "FLEX PLUS 10K UN",
    "FLEX PLUS 115K",
    "FLEX PLUS 120K",
    "FLEX PLUS 130K",
    "FLEX PLUS 150K",
    "FLEX PLUS 15K",
    "FLEX PLUS 175K",
    "FLEX PLUS 200K",
    "FLEX PLUS 20K",
    "FLEX PLUS 20K UN",
    "FLEX PLUS 250K",
    "FLEX PLUS 25K",
    "FLEX PLUS 30K",
    "FLEX PLUS 30K UN",
    "FLEX PLUS 35K",
    "FLEX PLUS 3K",
    "FLEX PLUS 40K",
    "FLEX PLUS 4K",
    "FLEX PLUS 50K",
    "FLEX PLUS 55K",
    "FLEX PLUS 5K",
    "FLEX PLUS 5K UN",
    "FLEX PLUS 60K",
    "FLEX PLUS 65K",
    "FLEX PLUS 70K",
    "FLEX PLUS 75K",
    "FLEX PLUS 80K",
    "FLEX PLUS 85K",
    "FLEX PLUS 90K",
    "FLEX PREMIUM 100K",
    "FLEX PREMIUM 25K",
    "FLEX PREMIUM 2K",
    "FLEX PREMIUM 30K",
    "FLEX PREMIUM 40K",
    "FLEX PREMIUM 50K",
    "FLEX PREMIUM 75K",
    "FLYBOX SIM",
    "FORFAIT MIX BRONZE",
    "FORFAIT MIX DIAMOND",
    "FORFAIT MIX GOLD",
    "FORFAIT MIX PLATINUM",
    "FORFAIT MIX SILVER",
    "FORFAIT SELECT",
    "FORFAIT SELECT PREMIUM",
    "GEOPRO",
    "GEOPRO PREMIUM",
    "GPRS TRACKING",
    "HOTLINE ENTREPRISE VERT",
    "HYBRID ORANGE PRO BUNDLE PLUS",
    "MODULO CLASSIQUE",
    "ORANGE BUSINESS FLEX MODULO",
    "ORANGE FLYBOX PRO",
    "ORANGE GRAND COMPTE MOVE",
    "ORANGE GRAND COMPTE MOVE MIX",
    "ORANGE MIX CORPORATE",
    "ORANGE MIX HONORABLE",
    "ORANGE MIX SCHLUMBERGER",
    "ORANGE MIX TOTAL",
    "ORANGE PREMIUM GC MIXTE",
    "ORANGE PRO L",
    "ORANGE PRO M",
    "ORANGE PRO S",
    "ORANGE PRO XL",
    "PORTHOS",
    "PREPAID CLOUD PRO",
    "PREPAID DATALIVE",
    "PREPAID FLEX PLUS",
    "PREPAID FORFAITDATA",
    "PREPAID ORANGE PRO BUNDLE",
    "PREPAID PRO",
    "PREPAID VOICE BULK",
    "ROUTAGE APPELS INTER",
    "SIM OPEL MAIN PRODUCT",
    "SMARTRACK",
    "WHITELIST 40G FIGEC")
    )B
    ON subs.acc_nbr=B.access_key
    left join 
    (
        select acct_id, 
            acct_res_id, 
            GROSS_BAL, 
            RESERVE_BAL, 
            CONSUME_BAL, 
            exp_date  
        from CDR.SPARK_IT_ZTE_BAL_EXTRACT 
        where  original_file_date = '###SLICE_VALUE###' and acct_res_id in ('180','50','131','37','183','61','64','253','2534','35','58','59','60','62','69','89','91','165','195','200','201','202','206','218','222','242','259','1234','1434','1','73','78','74','75')
    ) R on subs.acct_id = R.acct_id

) RT
WHERE STD_CODE IS NOT NULL