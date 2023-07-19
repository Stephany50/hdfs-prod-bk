INSERT INTO MON.SPARK_FT_CBM_DAILY_USAGE_SOLDE_DATA
SELECT DISTINCT
    NVL(A.acc_nbr, nvl(C.MSISDN, NVL(SUBS.MSISDN, CRA.served_party_msisdn))) AS MSISDN,
    NVL(B.acct_id, A.acct_id) AS ACCT_ID, 
    NVL(B.acct_res_id, NVL(C.BEN_ACCT_ID, NVL(CRA.USED_ACCT_RES_ID, SUBS.ACCT_RES_ID))) AS ACCT_RES_ID,
    NVL(B.bal_id, CRA.USED_BAL_ID)  AS BAL_ID,
    -- BAL_EXTRACT
    B.acct_res_name AS BAL_E_ACCT_RES_NAME,
    B.acct_res_rating_service_code AS BAL_E_ACCT_RES_RATING_SERVICE_CODE,
    ABS(IF(B.acct_res_id = 1, B.remain_bal_start, B.remain_bal_start/(1024*1024))) AS BAL_E_REMAIN_BAL_START,
    ABS(IF(B.acct_res_id = 1, B.remain_bal_end, B.remain_bal_end/(1024*1024))) AS BAL_E_REMAIN_BAL_END,
    date_format(B.eff_date, 'yyyy-MM-dd HH:mm:ss') AS BAL_E_EFF_DATE,
    date_format(B.exp_date, 'yyyy-MM-dd HH:mm:ss') AS BAL_E_EXP_DATE,
    -- DEPOT RECURRENT PRIS DANS IT_ZTE_RECURRING
    C.PROFILE AS DR_PROFILE, 
    C.DEPOT AS DR_DEPOT, 
    ABS(IF(C.BEN_ACCT_ID = 1, C.BEN_ACCT_ADD_RESULT, C.BEN_ACCT_ADD_RESULT/(1024*1024))) AS DR_BEN_ACCT_ADD_RESULT,
    ABS(IF(C.BEN_ACCT_ID = 1, C.BEN_ACCT_ADD_VAL, C.BEN_ACCT_ADD_VAL/(1024*1024))) AS DR_BEN_ACCT_ADD_VAL,
    C.USAGE_NORMAL AS DR_USAGE, 
    C.UNITE AS DR_UNITE, 
    date_format(C.BEN_ACCT_ADD_ACT_DATE, 'yyyy-MM-dd HH:mm:ss') AS DR_BEN_ACCT_ADD_ACT_DATE,
    date_format(C.BEN_ACCT_ADD_EXP_DATE, 'yyyy-MM-dd HH:mm:ss') AS DR_BEN_ACCT_ADD_EXP_DATE,
    -- DEPOT VIA SOUSCRIPTION PRIS DANS FT_SUBSCRIPTION
    SOUSCRIPT.RATED_AMOUNT AS SOUSCRIP_RATED_AMOUNT, 
    SOUSCRIPT.NBR_SUSCRIPTION AS SOUSCRIP_RATED_AMOUNT, 
    ABS(IF(SUBS.ACCT_RES_ID = 1, SUBS.BEN_ACCT_ADD_VAL, SUBS.BEN_ACCT_ADD_VAL/(1024*1024))) AS SUBSCRIP_BEN_ACCT_ADD_VAL, 
    date_format(SUBS.BEN_ACCT_ADD_ACT_DATE, 'yyyy-MM-dd HH:mm:ss') AS SUBSCRIP_BEN_ACCT_ADD_ACT_DATE,
    date_format(SUBS.BEN_ACCT_ADD_EXP_DATE, 'yyyy-MM-dd HH:mm:ss') AS SUBSCRIP_BEN_ACCT_ADD_EXP_DATE,
    -- USAGE DATA PRIS DANS CRA GPRS 
    CRA.USED_BALANCE AS CRA_USED_BALANCE,
    IF(CRA.USED_ACCT_RES_ID = 1, CRA.bytes_sent, CRA.bytes_sent/(1024*1024)) AS CRA_MO_SENT,
    IF(CRA.USED_ACCT_RES_ID = 1, CRA.bytes_received, CRA.bytes_received/(1024*1024)) AS CRA_MO_RECEIVED,
    IF(CRA.USED_ACCT_RES_ID = 1, CRA.charge_sum, CRA.charge_sum/(1024*1024)) AS CRA_MO_CHARGE_SUM,
    IF(CRA.USED_ACCT_RES_ID = 1, CRA.used_volume, CRA.used_volume/(1024*1024)) AS CRA_MO_USED_VOLUME,
    -- INSERTED_DATE AND EVENT_DATE
    CURRENT_TIMESTAMP AS INSERT_DATE,
    '###SLICE_VALUE###' AS EVENT_DATE

FROM 
-- BAL_EXTRACT: FULL JOIN ENTRE ENTRE LES BALANCES EN DEBUT DE JOURNEE DU JOUR J ET EN DEBUT DE JOURNEE DU JOUR J+1
    (
    SELECT 
        NVL(BAL1.bal_id, BAL2.bal_id) bal_id, 
        NVL(BAL1.acct_id, BAL2.acct_id) acct_id, 
        NVL(BAL1.acct_res_id, BAL2.acct_res_id) acct_res_id, 
        NVL(BAL1.acct_res_name, BAL2.acct_res_name) acct_res_name, 
        NVL(BAL1.acct_res_rating_service_code, BAL2.acct_res_rating_service_code) acct_res_rating_service_code, 
        NVL(BAL1.remain_bal, 0) remain_bal_start, 
        NVL(BAL2.remain_bal, 0) remain_bal_end, 
        NVL(BAL1.eff_date, BAL2.eff_date) eff_date, 
        NVL(BAL1.exp_date, BAL2.exp_date) exp_date 
    FROM
        (-- BAL_EXTRACT A J-1
        SELECT DISTINCT
            bal_id,
            acct_id,
            A1.acct_res_id,
            A2.acct_res_name,
            A2.acct_res_rating_service_code,
            remain_bal,
            eff_date,
            exp_date
        FROM 
            (
            SELECT 
                ( NVL(gross_bal, 0) + NVL(consume_bal, 0) + NVL(reserve_bal, 0) ) remain_bal,
                bal_id,
                acct_id,
                acct_res_id,
                eff_date,
                exp_date                 
            FROM CDR.SPARK_IT_ZTE_BAL_EXTRACT
            WHERE original_file_date = '###SLICE_VALUE###' 
                AND DATE_FORMAT(eff_date, 'yyyy-MM-dd') < DATE_ADD('###SLICE_VALUE###', 1) 
                AND DATE_FORMAT(exp_date, 'yyyy-MM-dd') >= DATE_FORMAT('###SLICE_VALUE###', 'yyyy-MM-dd 00:00:00')
            ) A1 
            LEFT JOIN  DIM.spark_DT_BALANCE_TYPE_ITEM A2 on A1.acct_res_id = A2.acct_res_id
        ) BAL1
        FULL JOIN 
        (-- BAL_EXTRACT A J
        SELECT DISTINCT
            bal_id,
            acct_id,
            A1.acct_res_id,
            A2.acct_res_name,
            A2.acct_res_rating_service_code,
            remain_bal,
            eff_date,
            exp_date
        FROM 
            (
            SELECT 
                ( NVL(gross_bal, 0) + NVL(consume_bal, 0) + NVL(reserve_bal, 0) ) remain_bal,
                bal_id,
                acct_id,
                acct_res_id,
                eff_date,
                exp_date                 
            FROM CDR.SPARK_IT_ZTE_BAL_EXTRACT
            WHERE original_file_date = DATE_ADD('###SLICE_VALUE###', 1) 
                AND date_format(eff_date, 'yyyy-MM-dd') < DATE_ADD('###SLICE_VALUE###', 1) 
                AND date_format(exp_date, 'yyyy-MM-dd') >= date_format('###SLICE_VALUE###', 'yyyy-MM-dd 00:00:00')
            ) A1
            LEFT JOIN  
            DIM.spark_DT_BALANCE_TYPE_ITEM A2 on A1.acct_res_id = A2.acct_res_id
        )BAL2
        ON  (BAL1.bal_id = BAL2.bal_id and BAL1.acct_res_rating_service_code = BAL2.acct_res_rating_service_code)
    ) B

    LEFT JOIN -- SUBS_EXTRACT

    (
    SELECT DISTINCT
        acct_id,
        acc_nbr
    FROM CDR.SPARK_IT_ZTE_SUBS_EXTRACT
    WHERE original_file_date = DATE_ADD('###SLICE_VALUE###', 1) 
    ) A
    ON A.acct_id = B.acct_id

    FULL JOIN -- DEPOT RECURRENT

    ( 
    SELECT
        MSISDN,
        BAL_ID,
        STD_CODE BEN_ACCT_ID,
        A2.acct_res_rating_service_code ACCT_RES_RATING_SERVICE_CODE,
        A2.acct_res_name ACCT_RES_NAME,
        PROFILE,
        MAIN_CREDIT,
        BEN_ACCT_ADD_RESULT,
        BEN_ACCT_ADD_VAL,
        FROM_UNIXTIME(UNIX_TIMESTAMP(BEN_ACCT_ADD_ACT_DATE, 'yyyyMMddHHmmss')) BEN_ACCT_ADD_ACT_DATE,
        FROM_UNIXTIME(UNIX_TIMESTAMP(BEN_ACCT_ADD_EXP_DATE, 'yyyyMMddHHmmss')) BEN_ACCT_ADD_EXP_DATE,
        ( CASE
            WHEN RT_1.USAGE like '%Compte%' THEN DEPOT/100
            WHEN STD_CODE ='253' THEN DEPOT
            WHEN STD_CODE IN ('187','60','195','200') THEN DEPOT/100
            WHEN STD_CODE IN ('165','242','2534') THEN DEPOT
            WHEN STD_CODE IN ('131','89') THEN DEPOT/100
            WHEN RT_1.USAGE like '%International%' THEN DEPOT/60
            WHEN RT_1.USAGE like '%Data%' THEN DEPOT/(1024*1024)
            WHEN RT_1.USAGE like '%SMS%' THEN DEPOT
            WHEN RT_1.USAGE like '%Roaming%' THEN DEPOT/60
            ELSE DEPOT END 
        ) DEPOT,
        RT_1.USAGE USAGE_NORMAL,
        A2.USAGE USAGE_DIM,
        ( CASE
            WHEN RT_1.USAGE like '%Compte%' THEN 'U'
            WHEN RT_1.USAGE like '%Local%' THEN 'U'
            WHEN RT_1.USAGE like '%Onnet%' THEN 'U'
            WHEN RT_1.USAGE like '%International%' THEN 'Min'
            WHEN RT_1.USAGE like '%Data%' THEN 'Mo'
            WHEN RT_1.USAGE like '%SMS%' THEN 'Nb'
            WHEN RT_1.USAGE like '%Roaming%' THEN 'Min'
            ELSE 'Autres' END 
        ) UNITE,
        EVENT_DATE
    FROM 
        (
        SELECT
            MSISDN,
            BAL_ID,
            BEN_ACCT_ID STD_CODE,
            MAIN MAIN_CREDIT,
            VAL_2 BEN_ACCT_ADD_RESULT,
            VAL_1 BEN_ACCT_ADD_VAL,
            PROFILE,
            DATE_AVANT BEN_ACCT_ADD_ACT_DATE,
            DATE_APRES BEN_ACCT_ADD_EXP_DATE,
            EVENT_DATE,
            ( CASE
            WHEN BEN_ACCT_ID IN ('1','73','78','36','38','1034','206') THEN VAL_1
            WHEN BEN_ACCT_ID IN ('187','50','131','37','183','61','64','253','2534','35','58','59','60','62','69','89','91','165','195','200','201','202','218','222','242','259','1234','1434') THEN VAL_2
            WHEN BEN_ACCT_ID IN ('74','75') AND  UPPER(PROFILE)  rlike '(FLEX [123456789]?[0-9]?[0-9](\.[05])?K)' THEN VAL_1
            WHEN BEN_ACCT_ID IN ('74','75') AND  UPPER(PROFILE) rlike '(FLEX +([a-zA-Z]))' THEN VAL_2
            ELSE 0 END ) DEPOT,
            ( CASE
            WHEN BEN_ACCT_ID = '1' THEN 'Compte principal'
            WHEN BEN_ACCT_ID IN ('50','75','37','35','59','62','69','91','206','218','222','259','1234','1434','1034','36','38') THEN 'Data'
            WHEN BEN_ACCT_ID IN ('183','61','73','58','201','202') THEN 'SMS'
            WHEN BEN_ACCT_ID IN ('187','253','60','195','200') THEN 'Voix Local'
            WHEN BEN_ACCT_ID IN ('131','2534','89','165','242') THEN 'Voix Onnet'
            WHEN BEN_ACCT_ID  IN ('64','74') THEN 'Voix International'
            WHEN BEN_ACCT_ID = '78' THEN 'Voix Roaming'
            ELSE 'Autres' END ) USAGE
        FROM 
            (
            SELECT
                R2.MSISDN MSISDN,
                BAL_ID,
                BEN_ACCT_ID,
                B.main_credit MAIN,
                R2.BEN_ACCT_ADD_RESULT VAL_2,
                R2.BEN_ACCT_ADD_VAL VAL_1,
                B.commercial_offer PROFILE,
                R2.BEN_ACCT_ADD_ACT_DATE DATE_AVANT,
                R2.BEN_ACCT_ADD_EXP_DATE DATE_APRES,
                R2.EVENT_DATE
            FROM 
                (
                SELECT
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
                    SELECT 
                        A.*, 
                        ROW_NUMBER() OVER(ORDER BY EVENT_DATE) ID
                    FROM CDR.SPARK_IT_ZTE_RECURRING A
                    WHERE A.EVENT_DATE = '###SLICE_VALUE###' 
                    ) A
                    LATERAL VIEW EXPLODE(SPLIT(NVL(BENEFIT_BAL_LIST, ''), '\;')) TMP AS BEN_BAL
                ) R2

                INNER JOIN

                (
                SELECT 
                    main_credit,
                    commercial_offer,
                    access_key, 
                    event_date 
                FROM MON.SPARK_FT_CONTRACT_SNAPSHOT
                WHERE EVENT_DATE  = DATE_ADD('###SLICE_VALUE###', 1)
                )B
                ON R2.MSISDN=B.access_key 
            ) RT 
            where cast(BEN_ACCT_ID as INT) <> 1
        ) RT_1

        LEFT JOIN  
        DIM.spark_DT_BALANCE_TYPE_ITEM A2 on RT_1.STD_CODE = A2.acct_res_id
    ) C
    ON B.bal_id = C.BAL_ID

    FULL JOIN 
    -- SUBSCRIPTION

    (
    SELECT 
        MSISDN, 
        TRANSACTION_DATE, 
        ACCT_RES_ID, 
        sum(rated_amount) RATED_AMOUNT, 
        count(ACCT_RES_ID) NBR_SUSCRIPTION, 
        sum(BEN_ACCT_ADD_VAL) BEN_ACCT_ADD_VAL, 
        min(BEN_ACCT_ADD_ACT_DATE) BEN_ACCT_ADD_ACT_DATE, 
        max(BEN_ACCT_ADD_EXP_DATE) BEN_ACCT_ADD_EXP_DATE
    from 
        (
            SELECT
                fn_format_msisdn_to_9digits(SERVED_PARTY_MSISDN) MSISDN,
                transaction_date, 
                transaction_time, 
                transactionsn, 
                bal_id ACCT_RES_ID_LIST, 
                benefit_balance_list BENEFIT_BALANCE_LIST, 
                service_list SERVICE_LIST,
                rated_amount rated_amount, 
                SPLIT(BEN_BAL, '&')[0] ACCT_RES_ID,
                CAST(ABS(CAST(SPLIT(BEN_BAL, '&')[1] AS DOUBLE)) AS STRING) BEN_ACCT_ADD_VAL,
                CAST(ABS(CAST(SPLIT(BEN_BAL, '&')[2] AS DOUBLE)) AS STRING) BEN_ACCT_ADD_RESULT,
                SPLIT(BEN_BAL, '&')[3] BEN_ACCT_ADD_ACT_DATE,
                SPLIT(BEN_BAL, '&')[4] BEN_ACCT_ADD_EXP_DATE
            FROM 
                (
                SELECT 
                    transaction_date, 
                    transaction_time, 
                    served_party_msisdn, 
                    transactionsn, bal_id, 
                    benefit_balance_list, 
                    benefit_unit_list, 
                    combo, 
                    benefit_bal_list, 
                    service_list, 
                    rated_amount
                FROM MON.SPARK_FT_SUBSCRIPTION
                WHERE TRANSACTION_DATE = '###SLICE_VALUE###'
                ) A
                LATERAL VIEW EXPLODE(SPLIT(NVL(BENEFIT_BAL_LIST, ''), '#')) TMP AS BEN_BAL
        ) FTS
    group by MSISDN, TRANSACTION_DATE, 
    ACCT_RES_ID
    ) SUBS 
    ON (A.acc_nbr = SUBS.MSISDN and B.acct_res_id = SUBS.ACCT_RES_ID) -- jointure avec MSISDN et ACCT_RES_ID 

    FULL JOIN 
    -- USAGE DATA CRA_GPRS

    (
    SELECT 
        SESSION_DATE,
        SERVED_PARTY_MSISDN,
        USED_BAL_ID,
        USED_ACCT_RES_ID,
        USED_BALANCE,
        sum(bytes_sent) bytes_sent,
        sum(bytes_received) bytes_received,
        sum(charge_sum) charge_sum,
        sum(used_volume) used_volume
    from 
        (
            select 
                SESSION_DATE, SESSION_TIME, SERVED_PARTY_MSISDN, BYTES_SENT, BYTES_RECEIVED, CHARGE_SUM, 
                SPLIT(used_volume_list, '\\|')[0] used_volume, 
                SPLIT(used_balance_list, '\\|')[0] used_balance, 
                SPLIT(remaining_volume_list, '\\|')[0] remaining_volume, 
                SPLIT(preceding_volume_list, '\\|')[0] preceding_volume,  
                SPLIT(USED_BAL_ID_LIST, '\\|')[0] USED_BAL_ID,   
                SPLIT(USED_ACCT_RES_ID_LIST, '\\|')[0] USED_ACCT_RES_ID,   
                SPLIT(USED_ACCT_ITEM_TYPE_ID_LIST, '\\|')[0] USED_ACCT_ITEM_TYPE_ID 
            from MON.SPARK_ft_cra_gprs_with_bal_id 
            where session_date = "###SLICE_VALUE###"
            and cast(SPLIT(used_volume_list, '\\|')[0] as DOUBLE) > 0
            and SERVED_PARTY_MSISDN is not null

            union all 

            select 
                SESSION_DATE, SESSION_TIME, SERVED_PARTY_MSISDN, BYTES_SENT, BYTES_RECEIVED, CHARGE_SUM, 
                SPLIT(used_volume_list, '\\|')[1] used_volume, 
                SPLIT(used_balance_list, '\\|')[1] used_balance, 
                SPLIT(remaining_volume_list, '\\|')[1] remaining_volume, 
                SPLIT(preceding_volume_list, '\\|')[1] preceding_volume,  
                SPLIT(USED_BAL_ID_LIST, '\\|')[1] USED_BAL_ID,   
                SPLIT(USED_ACCT_RES_ID_LIST, '\\|')[1] USED_ACCT_RES_ID,   
                SPLIT(USED_ACCT_ITEM_TYPE_ID_LIST, '\\|')[1] USED_ACCT_ITEM_TYPE_ID 
            from MON.SPARK_ft_cra_gprs_with_bal_id 
            where session_date = "###SLICE_VALUE###"
            and cast(SPLIT(used_volume_list, '\\|')[1] as DOUBLE) > 0
            and SERVED_PARTY_MSISDN is not null

            union all

            select 
                SESSION_DATE, SESSION_TIME, SERVED_PARTY_MSISDN, BYTES_SENT, BYTES_RECEIVED, CHARGE_SUM, 
                SPLIT(used_volume_list, '\\|')[2] used_volume,
                SPLIT(used_balance_list, '\\|')[2] used_balance, 
                SPLIT(remaining_volume_list, '\\|')[2] remaining_volume,
                SPLIT(preceding_volume_list, '\\|')[2] preceding_volume,  
                SPLIT(USED_BAL_ID_LIST, '\\|')[2] USED_BAL_ID,
                SPLIT(USED_ACCT_RES_ID_LIST, '\\|')[2] USED_ACCT_RES_ID,
                SPLIT(USED_ACCT_ITEM_TYPE_ID_LIST, '\\|')[2] USED_ACCT_ITEM_TYPE_ID 
            from MON.SPARK_ft_cra_gprs_with_bal_id 
            where session_date = "###SLICE_VALUE###"
            and cast(SPLIT(used_volume_list, '\\|')[2] as DOUBLE) > 0
            and SERVED_PARTY_MSISDN is not null

            union all

            select 
                SESSION_DATE, SESSION_TIME, SERVED_PARTY_MSISDN, BYTES_SENT, BYTES_RECEIVED, CHARGE_SUM, 
                SPLIT(used_volume_list, '\\|')[3] used_volume,
                SPLIT(used_balance_list, '\\|')[3] used_balance,
                SPLIT(remaining_volume_list, '\\|')[3] remaining_volume,
                SPLIT(preceding_volume_list, '\\|')[3] preceding_volume,
                SPLIT(USED_BAL_ID_LIST, '\\|')[3] USED_BAL_ID,
                SPLIT(USED_ACCT_RES_ID_LIST, '\\|')[3] USED_ACCT_RES_ID,
                SPLIT(USED_ACCT_ITEM_TYPE_ID_LIST, '\\|')[3] USED_ACCT_ITEM_TYPE_ID 
            from MON.SPARK_ft_cra_gprs_with_bal_id 
            where session_date = "###SLICE_VALUE###"
            and cast(SPLIT(used_volume_list, '\\|')[3] as DOUBLE) > 0
            and SERVED_PARTY_MSISDN is not null
        ) CRA
    group by session_date, served_party_msisdn, used_bal_id, used_acct_res_id, USED_BALANCE
    ) CRA
    ON B.BAL_ID = CRA.USED_BAL_ID

    WHERE 
    upper(B.acct_res_name) like "%DATA%" or 
    (B.acct_res_id = 1 and upper(B.acct_res_rating_service_code) like '%DATA%')
