INSERT INTO MON.SPARK_SOLDE_CLIENT_B2B

select
    subs.acc_nbr MSISDN,
    B.commercial_offer,
    B.main_credit,
    -(nvl(sms_bundle.GROSS_BAL, 0) + nvl(sms_bundle.RESERVE_BAL, 0) + nvl(sms_bundle.CONSUME_BAL, 0)) sms_bundle_pro_balance,
    sms_bundle.exp_date sms_bundle_pro_Expiry_Date,
    -(nvl(sms_cross_pro.GROSS_BAL, 0) + nvl(sms_cross_pro.RESERVE_BAL, 0) + nvl(sms_cross_pro.CONSUME_BAL, 0)) sms_cross_net_pro_balance,
    sms_cross_pro.exp_date sms_cross_net_pro_Expiry_Date,
    -(nvl(sms_cross_net.GROSS_BAL, 0) + nvl(sms_cross_net.RESERVE_BAL, 0) + nvl(sms_cross_net.CONSUME_BAL, 0)) sms_cross_net_balance,
    sms_cross_net.exp_date sms_cross_net_Expiry_Date,

    -(nvl(Voice_Allnet_E237.GROSS_BAL, 0) + nvl(Voice_Allnet_E237.RESERVE_BAL, 0) + nvl(Voice_Allnet_E237.CONSUME_BAL, 0)) Voice_Allnet_E237_balance,
    Voice_Allnet_E237.exp_date Voice_Allnet_E237_Expiry_Date,
    -(nvl(Benefit_Crossnet_Safari.GROSS_BAL, 0) + nvl(Benefit_Crossnet_Safari.RESERVE_BAL, 0) + nvl(Benefit_Crossnet_Safari.CONSUME_BAL, 0)) Benefit_Crossnet_Safari_balance,
    Benefit_Crossnet_Safari.exp_date Benefit_Crossnet_Safari_Expiry_Date,
    -(nvl(Voice_Onnet_E237.GROSS_BAL, 0) + nvl(Voice_Onnet_E237.RESERVE_BAL, 0) + nvl(Voice_Onnet_E237.CONSUME_BAL, 0)) Voice_Onnet_E237_balance,
    Voice_Onnet_E237.exp_date Voice_Onnet_E237_Expiry_Date,
    -(nvl(Bundle_Money_Pro.GROSS_BAL, 0) + nvl(Bundle_Money_Pro.RESERVE_BAL, 0) + nvl(Bundle_Money_Pro.CONSUME_BAL, 0)) Bundle_Money_Pro_balance,
    Bundle_Money_Pro.exp_date Bundle_Money_Pro_Expiry_Date,
    -(nvl(Credit_Onnet.GROSS_BAL, 0) + nvl(Credit_Onnet.RESERVE_BAL, 0) + nvl(Credit_Onnet.CONSUME_BAL, 0)) Credit_Onnet_balance,
    Credit_Onnet.exp_date Credit_Onnet_Expiry_Date,

    -(nvl(Bonus_Data.GROSS_BAL, 0) + nvl(Bonus_Data.RESERVE_BAL, 0) + nvl(Bonus_Data.CONSUME_BAL, 0)) Bonus_Data_balance,
    Bonus_Data.exp_date Bonus_Data_Expiry_Date,
    -(nvl(Data_mobile_internet.GROSS_BAL, 0) + nvl(Data_mobile_internet.RESERVE_BAL, 0) + nvl(Data_mobile_internet.CONSUME_BAL, 0)) Data_mobile_internet_balance,
    Data_mobile_internet.exp_date Data_mobile_internet_Expiry_Date,
    -(nvl(Data_All_Browsing_Pro.GROSS_BAL, 0) + nvl(Data_All_Browsing_Pro.RESERVE_BAL, 0) + nvl(Data_All_Browsing_Pro.CONSUME_BAL, 0)) Data_All_Browsing_Pro_balance,
    Data_All_Browsing_Pro.exp_date Data_All_Browsing_Pro_Expiry_Date,

   
    -(nvl(Voice_International.GROSS_BAL, 0) + nvl(Voice_International.RESERVE_BAL, 0) + nvl(Voice_International.CONSUME_BAL, 0)) Voice_International_balance,
    Voice_International.exp_date Voice_International_Expiry_Date,
    -(nvl(Voice_International_Pro.GROSS_BAL, 0) + nvl(Voice_International_Pro.RESERVE_BAL, 0) + nvl(Voice_International_Pro.CONSUME_BAL, 0)) Voice_International_Pro_balance,
    Voice_International_Pro.exp_date Voice_International_Pro_Expiry_Date,
    -(nvl(Voice_Roaming_Pro.GROSS_BAL, 0) + nvl(Voice_Roaming_Pro.RESERVE_BAL, 0) + nvl(Voice_Roaming_Pro.CONSUME_BAL, 0)) Voice_Roaming_Pro_balance,
    Voice_Roaming_Pro.exp_date Voice_Roaming_Pro_Expiry_Date,

    CURRENT_TIMESTAMP() INSERT_DATE,
    subs.ORIGINAL_FILE_DATE EVENT_DATE
from 
(
    select distinct acc_nbr, 
        acct_id, 
        original_file_date 
    from CDR.SPARK_IT_ZTE_SUBS_EXTRACT 
    where original_file_date = '2023-01-23'
) subs
INNER JOIN 

(SELECT main_credit,commercial_offer,access_key FROM MON.SPARK_FT_CONTRACT_SNAPSHOT 
WHERE EVENT_DATE='2023-01-24' AND  upper(commercial_offer) IN 
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
)B ON subs.acc_nbr=B.access_key

left join 
(
    select acct_id, 
        acct_res_id, 
        GROSS_BAL, 
        RESERVE_BAL, 
        CONSUME_BAL, 
        exp_date  
    from CDR.SPARK_IT_ZTE_BAL_EXTRACT 
    where acct_res_id = 50 and original_file_date = '2023-01-23'
) Bonus_Data on subs.acct_id = Bonus_Data.acct_id
left join 
(
    select acct_id, 
        acct_res_id, 
        GROSS_BAL, 
        RESERVE_BAL, 
        CONSUME_BAL, 
        exp_date  
    from CDR.SPARK_IT_ZTE_BAL_EXTRACT 
    where acct_res_id = 75 and original_file_date = '2023-01-23'
) Data_All_Browsing_Pro on subs.acct_id = Data_All_Browsing_Pro.acct_id
left join 
(
    select acct_id, 
        acct_res_id, 
        GROSS_BAL, 
        RESERVE_BAL, 
        CONSUME_BAL, 
        exp_date  
    from CDR.SPARK_IT_ZTE_BAL_EXTRACT 
    where acct_res_id = 242 and original_file_date = '2023-01-23'
) Voice_Allnet_E237 on subs.acct_id = Voice_Allnet_E237.acct_id
left join 
(
    select acct_id, 
        acct_res_id, 
        GROSS_BAL, 
        RESERVE_BAL, 
        CONSUME_BAL, 
        exp_date  
    from CDR.SPARK_IT_ZTE_BAL_EXTRACT 
    where acct_res_id = 183 and original_file_date = '2023-01-23'
) Benefit_Crossnet_Safari on subs.acct_id = Benefit_Crossnet_Safari.acct_id
left join 
(
    select acct_id, 
        acct_res_id, 
        GROSS_BAL, 
        RESERVE_BAL, 
        CONSUME_BAL, 
        exp_date  
    from CDR.SPARK_IT_ZTE_BAL_EXTRACT 
    where acct_res_id = 185 and original_file_date = '2023-01-23'
) sms_bundle on subs.acct_id = sms_bundle.acct_id
left join 
(
    select acct_id, 
        acct_res_id, 
        GROSS_BAL, 
        RESERVE_BAL, 
        CONSUME_BAL, 
        exp_date  
    from CDR.SPARK_IT_ZTE_BAL_EXTRACT 
    where acct_res_id = 73 and original_file_date = '2023-01-23'
) sms_cross_pro on subs.acct_id = sms_cross_pro.acct_id
left join 
(
    select acct_id, 
        acct_res_id, 
        GROSS_BAL, 
        RESERVE_BAL, 
        CONSUME_BAL, 
        exp_date  
    from CDR.SPARK_IT_ZTE_BAL_EXTRACT 
    where acct_res_id = 61 and original_file_date = '2023-01-23'
) sms_cross_net on subs.acct_id = sms_cross_net.acct_id
left join 
(
    select acct_id, 
        acct_res_id, 
        GROSS_BAL, 
        RESERVE_BAL, 
        CONSUME_BAL, 
        exp_date  
    from CDR.SPARK_IT_ZTE_BAL_EXTRACT 
    where acct_res_id = 2534 and original_file_date = '2023-01-23'
) Voice_Onnet_E237 on subs.acct_id = Voice_Onnet_E237.acct_id
left join 
(
    select acct_id, 
        acct_res_id, 
        GROSS_BAL, 
        RESERVE_BAL, 
        CONSUME_BAL, 
        exp_date  
    from CDR.SPARK_IT_ZTE_BAL_EXTRACT 
    where acct_res_id = 37 and original_file_date = '2023-01-23'
) Data_mobile_internet on subs.acct_id = Data_mobile_internet.acct_id
left join 
(
    select acct_id, 
        acct_res_id, 
        GROSS_BAL, 
        RESERVE_BAL, 
        CONSUME_BAL, 
        exp_date  
    from CDR.SPARK_IT_ZTE_BAL_EXTRACT 
    where acct_res_id = 131 and original_file_date = '2023-01-23'
) Credit_Onnet on subs.acct_id = Credit_Onnet.acct_id
left join 
(
    select acct_id, 
        acct_res_id, 
        GROSS_BAL, 
        RESERVE_BAL, 
        CONSUME_BAL, 
        exp_date  
    from CDR.SPARK_IT_ZTE_BAL_EXTRACT 
    where acct_res_id = 76 and original_file_date = '2023-01-23'
) Bundle_Money_Pro on subs.acct_id = Bundle_Money_Pro.acct_id

left join 
(
    select acct_id, 
        acct_res_id, 
        GROSS_BAL, 
        RESERVE_BAL, 
        CONSUME_BAL, 
        exp_date  
    from CDR.SPARK_IT_ZTE_BAL_EXTRACT 
    where acct_res_id = 64 and original_file_date = '2023-01-23'
) Voice_International on subs.acct_id = Voice_International.acct_id
left join 
(
    select acct_id, 
        acct_res_id, 
        GROSS_BAL, 
        RESERVE_BAL, 
        CONSUME_BAL, 
        exp_date  
    from CDR.SPARK_IT_ZTE_BAL_EXTRACT 
    where acct_res_id = 74 and original_file_date = '2023-01-23'
) Voice_International_Pro on subs.acct_id = Voice_International_Pro.acct_id
left join 
(
    select acct_id, 
        acct_res_id, 
        GROSS_BAL, 
        RESERVE_BAL, 
        CONSUME_BAL, 
        exp_date  
    from CDR.SPARK_IT_ZTE_BAL_EXTRACT 
    where acct_res_id = 78 and original_file_date = '2023-01-23'
) Voice_Roaming_Pro on subs.acct_id = Voice_Roaming_Pro.acct_id


