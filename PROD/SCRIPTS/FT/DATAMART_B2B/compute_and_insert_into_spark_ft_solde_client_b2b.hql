INSERT INTO MON.SPARK_SOLDE_CLIENT_B2B
SELECT 
MSISDN,
RP.code_std STD_CODE,
PROFILE,
SOLDE,
USAGE,
UNITE,
EVENT_DATE
FROM 
(
    SELECT
    MSISDN,
    STD_CODE,
    PROFILE,
    ( CASE 
        WHEN USAGE like '%Compte%' THEN 'U'
        WHEN USAGE like '%Local%' THEN 'U'
        WHEN USAGE like '%Onnet%' THEN 'U'
        WHEN USAGE like '%International%' THEN 'Min'
        WHEN USAGE like '%Data%' THEN 'Mo'
        WHEN USAGE like '%SMS%' THEN 'Nb'
        WHEN USAGE like '%Roaming%' THEN 'Min'
        WHEN USAGE like '%Usage%' THEN 'U'
    ELSE 'Autres' END ) AS UNITE,
    USAGE,
    ( CASE 
        WHEN USAGE like '%Compte%' THEN SOLDE/100
        WHEN USAGE like '%Usage%' THEN SOLDE/100
        WHEN STD_CODE ='242' THEN SOLDE
        WHEN STD_CODE IN ('183','60','193','106') THEN SOLDE/100
        WHEN STD_CODE IN ('165','231','2534') THEN SOLDE
        WHEN STD_CODE IN ('131','89') THEN SOLDE/100
        WHEN USAGE like '%International%' THEN SOLDE/60
        WHEN USAGE like '%Data%' THEN SOLDE/(1024*1024)
        WHEN USAGE like '%SMS%' THEN SOLDE
        WHEN USAGE like '%Roaming%' THEN SOLDE/60
    ELSE SOLDE END ) AS SOLDE,
    EVENT_DATE
    FROM
    ( select
            R.acc_nbr MSISDN,
            R.commercial_offer PROFILE,
            R.acct_res_id STD_CODE,
            -(nvl(R.GROSS_BAL, 0) + nvl(R.RESERVE_BAL, 0) + nvl(R.CONSUME_BAL, 0)) SOLDE,
            ( CASE 
                WHEN R.acct_res_id = '1' THEN 'Compte Principal'
                WHEN R.acct_res_id IN ('50','75','37','35','59','62','69','91','104','207','211','248','1234','1134','36','38','1034') THEN 'Data'
                WHEN R.acct_res_id IN ('185','61','73','58','101','99') THEN 'SMS'
                WHEN R.acct_res_id IN ('183','242','60','193','106') THEN 'Voix Local'
                WHEN R.acct_res_id IN ('131','2534','89','165','231') THEN 'Voix Onnet'
                WHEN R.acct_res_id IN ('64','74') THEN 'Voix International'
                WHEN R.acct_res_id = '78' THEN 'Voix Roaming'
                WHEN R.acct_res_id = '76' THEN 'All Usage'
            ELSE 'Autres' END ) USAGE,
            R.ORIGINAL_FILE_DATE EVENT_DATE
        from 
        (select
            subs.acc_nbr,
            R_1.acct_res_id, 
            R_1.GROSS_BAL, 
            R_1.RESERVE_BAL, 
            R_1.CONSUME_BAL,
            B.commercial_offer,
            subs.ORIGINAL_FILE_DATE ,
            row_number() over ( partition by subs.acc_nbr, R_1.acct_res_id order by R_1.update_date desc )rn,
            R_1.update_date,
            R_1.exp_date
            FROM
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
            (
                "ARAMIS",
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
                "WHITELIST 40G FIGEC"
            )
            )B
            ON subs.acc_nbr=B.access_key
            left join 
            (
                select acct_id, 
                    acct_res_id, 
                    GROSS_BAL, 
                    RESERVE_BAL, 
                    CONSUME_BAL,
                    update_date,
                    exp_date  
                from CDR.SPARK_IT_ZTE_BAL_EXTRACT 
                where  original_file_date = '###SLICE_VALUE###' and acct_res_id in ('1','35','36','37','38','50','58','59','60','61','62','64','69','73','74','75','76','78','89','91','131','158','165','185','183','193','106','101','99','104','207','211','231','242','248','1034','1234','1134','2534')
            ) R_1 on subs.acct_id = R_1.acct_id
    )R
    where rn=1 and R.acct_res_id is not null
    ) RT
)RF
LEFT JOIN (select code_std,id_std FROM DIM.REF_STD_CODE_B2B) RP
ON RF.STD_CODE = RP.id_std