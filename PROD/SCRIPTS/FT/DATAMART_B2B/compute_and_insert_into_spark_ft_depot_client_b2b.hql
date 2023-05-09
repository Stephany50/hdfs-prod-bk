INSERT INTO MON.SPARK_DEPOT_CLIENT_B2B

SELECT 
MSISDN,
STD_CODE,
PROFILE,
( CASE 
    WHEN USAGE like '%Compte%' THEN DEPOT/100
    WHEN USAGE like '%Local%' THEN DEPOT/100 
    WHEN USAGE like '%International%' THEN DEPOT/60
    WHEN USAGE like '%Data%' THEN DEPOT/(1024*1024*1024)
    WHEN USAGE like '%SMS%' THEN DEPOT
    WHEN USAGE like '%Roaming%' THEN DEPOT/60
ELSE DEPOT END ) DEPOT,
USAGE,
( CASE 
    WHEN USAGE like '%Compte%' THEN 'U'
    WHEN USAGE like '%Local%' THEN 'U'
    WHEN USAGE like '%International%' THEN 'Min'
    WHEN USAGE like '%Data%' THEN 'Go'
    WHEN USAGE like '%SMS%' THEN 'Nb'
    WHEN USAGE like '%Roaming%' THEN 'Min'
ELSE 'Autres' END ) UNITE,
DATES EVENT_DATE
FROM(
    SELECT 
    MSISDN,
    BEN_ACCT_ID STD_CODE,
    PROFILE,
    DATES,
    ( CASE 
        WHEN BEN_ACCT_ID IN ('1','73','78') THEN VAL_1
        WHEN BEN_ACCT_ID IN ('180','50','131','37','183','61','64','253','2534','35','58','59','60','62','69','89','91','165','195','200','201','202','206','218','222','242','259','1234','1434') THEN VAL_2
        WHEN BEN_ACCT_ID IN ('74','75') AND  UPPER(PROFILE)  rlike '(FLEX\s\d+(\.\d+)?K)' THEN VAL_1
        WHEN BEN_ACCT_ID IN ('74','75') AND  UPPER(PROFILE) rlike '(Flex\s+([a-zA-Z]))' THEN VAL_2
    ELSE 0 END ) DEPOT,

    ( CASE 
        WHEN BEN_ACCT_ID = '1' THEN 'Compte principal'
        WHEN BEN_ACCT_ID IN ('50','75','37','35','59','62','69','91','206','218','222','259','1234','1434') THEN 'Data'
        WHEN BEN_ACCT_ID IN ('183','61','73','58','201','202') THEN 'SMS'
        WHEN BEN_ACCT_ID IN ('187','253','60','195','200') THEN 'Voix Local'
        WHEN BEN_ACCT_ID IN ('131','2534','89','165','242') THEN 'Voix Onnet'
        WHEN BEN_ACCT_ID IN ('64','74') THEN 'Voix International'
        WHEN BEN_ACCT_ID = '78' THEN 'Voix Roaming'
    ELSE 'Autres' END ) USAGE
    FROM (
        SELECT 
        R2.MSISDN MSISDN,
        BEN_ACCT_ID,
        B.main_credit MAIN,
        R2.BEN_ACCT_ADD_RESULT VAL_2,
        R2.BEN_ACCT_ADD_VAL VAL_1,
        B.commercial_offer PROFILE,
        R2.BEN_ACCT_ADD_ACT_DATE DATE_AVANT,
        R2.BEN_ACCT_ADD_EXP_DATE DATE_APRES,
        DATES
        FROM
        (SELECT 
        MSISDN,
        MD_PACC,
        BEN_ACCT_ID,
        BEN_ACCT_ADD_RESULT,
        BEN_ACCT_ADD_VAL,
        BAL_ID,
        BEN_ACCT_ADD_ACT_DATE,
        BEN_ACCT_ADD_EXP_DATE,
        EVENT_DATE DATES,
        ID
        FROM
        (SELECT 
        SUBSTR(acc_nbr,4) MSISDN,
            event_cost_list/100 MD_PACC, 
            SPLIT(BEN_BAL, '&')[0] BEN_ACCT_ID,
            SPLIT(BEN_BAL, '&')[1] BAL_ID,
            CAST(ABS(CAST(SPLIT(BEN_BAL, '&')[2] AS DOUBLE)) AS STRING) BEN_ACCT_ADD_VAL,
            CAST(ABS(CAST(SPLIT(BEN_BAL, '&')[3] AS DOUBLE)) AS STRING) BEN_ACCT_ADD_RESULT,
            SPLIT(BEN_BAL, '&')[4] BEN_ACCT_ADD_ACT_DATE,
            SPLIT(BEN_BAL, '&')[5] BEN_ACCT_ADD_EXP_DATE,
            EVENT_DATE,
            ID
        FROM
            (
            SELECT A.*, ROW_NUMBER() OVER(ORDER BY EVENT_DATE) ID
            FROM CDR.SPARK_IT_ZTE_RECURRING A
            WHERE A.EVENT_DATE = '###SLICE_VALUE###'
            ) A
            LATERAL VIEW EXPLODE(SPLIT(NVL(BENEFIT_BAL_LIST, ''), '\;')) TMP AS BEN_BAL) R1
        WHERE BEN_ACCT_ID in ('180','50','131','37','183','61','64','253','2534','35','58','59','60','62','69','89','91','165','195','200','201','202','206','218','222','242','259','1234','1434','1','73','78','74','75')
        )R2
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

        ON R2.MSISDN=B.access_key
    ) RT 
) RT_1