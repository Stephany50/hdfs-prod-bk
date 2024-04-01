INSERT INTO MON.SPARK_FT_CBM_ADVANCED_DATA_TRAFIC_TDD_DAILY
    SELECT DISTINCT 
        A.imei,
        A.tac_code,
        terminal_brand,
        terminal_model,
        CASE WHEN typebox IS NOT NULL THEN typebox
             ELSE 'BOX GRISE' END AS typebox,
        trafic_data,
        nb_msisdn,
        msisdn1,
        profil1,
        trafic_msisdn_1,
        msisdn2,
        profil2,
        trafic_msisdn_2,
        msisdn3,
        profil3,
        trafic_msisdn_3,
        vol_streaming,
        vol_chat,
        vol_download,
        vol_games,
        vol_voip,
        vol_web,
        vol_others,
        contenu_pref,
        vol_contenu_pref,
        vol_2G,
        vol_3G,
        vol_LTE,
        techno_pref,
        vol_techno_pref,
        CURRENT_TIMESTAMP AS INSERT_DATE,
        '###SLICE_VALUE###' AS event_date
    FROM 
    (
        SELECT 
            imei,
            msisdn,
            tac_code,
            terminal_type,
            terminal_model,
            terminal_brand
        FROM MON.SPARK_FT_IMEI_CBM
        WHERE transaction_date = '###SLICE_VALUE###'
        AND tac_code IN (
            SELECT tac_code FROM DIM.TDD_TAC_CODE
            )
    ) A
    LEFT JOIN 
    (
        SELECT 
            imei,
            MAX(CASE WHEN rn = 1 THEN msisdn ELSE NULL END) AS msisdn1,
            MAX(CASE WHEN rn = 2 THEN msisdn ELSE NULL END) AS msisdn2,
            MAX(CASE WHEN rn = 3 THEN msisdn ELSE NULL END) AS msisdn3,
            MAX(CASE WHEN rn = 1 THEN commercial_offer ELSE NULL END) AS profil1,
            MAX(CASE WHEN rn = 2 THEN commercial_offer ELSE NULL END) AS profil2,
            MAX(CASE WHEN rn = 3 THEN commercial_offer ELSE NULL END) AS profil3,
            SUM(CASE WHEN rn = 1 THEN nbytest ELSE 0 END) AS trafic_msisdn_1,
            SUM(CASE WHEN rn = 2 THEN nbytest ELSE 0 END) AS trafic_msisdn_2,
            SUM(CASE WHEN rn = 3 THEN nbytest ELSE 0 END) AS trafic_msisdn_3
        FROM 
        (
            SELECT 
                B12.imei AS imei,
                B11.msisdn AS msisdn,
                nbytest,
                commercial_offer,
                ROW_NUMBER() OVER (PARTITION BY imei ORDER BY nbytest DESC) AS rn 
            FROM 
            (
                SELECT DISTINCT
                    msisdn,
                    SUM(nbytest) AS nbytest,
                    commercial_offer,
                    imei AS tac_code
                FROM MON.SPARK_FT_OTARIE_DATA_TRAFFIC_DAY
                WHERE transaction_date = '###SLICE_VALUE###'
                GROUP BY
                    msisdn,
                    imei,
                    commercial_offer
            ) B11
            LEFT JOIN 
            (
                SELECT DISTINCT
                    msisdn,
                    imei,
                    tac_code
                FROM MON.SPARK_FT_IMEI_CBM
                WHERE transaction_date = '###SLICE_VALUE###'
            ) B12 
            ON GET_NNP_MSISDN_9DIGITS(B11.msisdn) = GET_NNP_MSISDN_9DIGITS(B12.msisdn)
            AND B11.tac_code = B12.tac_code
        ) B1 
        WHERE rn <= 3
        GROUP BY imei
    ) B
    ON A.imei = B.imei
    LEFT JOIN
    (
        SELECT
            imei,
            SUM(trafic_data) AS trafic_data,
            COUNT(DISTINCT msisdn) AS nb_msisdn 
        FROM 
        (
            SELECT 
                C1.imei AS imei,
                C1.msisdn AS msisdn,
                trafic_data
            FROM 
            (
                SELECT 
                    imei,
                    msisdn,
                    tac_code
                FROM MON.SPARK_FT_IMEI_CBM
                WHERE transaction_date = '###SLICE_VALUE###'
            ) C1
            LEFT JOIN 
            (
                SELECT 
                    SUM(nbytest) AS trafic_data,
                    msisdn,
                    imei AS tac_code
                FROM MON.SPARK_FT_OTARIE_DATA_TRAFFIC_DAY
                WHERE transaction_date = '###SLICE_VALUE###'
                GROUP BY msisdn, imei
            ) C2
            ON C1.msisdn = C2.msisdn
            AND C1.tac_code = C2.tac_code
        ) C0
        GROUP BY imei
    ) C
    ON A.imei = C.imei
    LEFT JOIN 
    (
        SELECT 
            D1.imei,
            vol_streaming,
            vol_chat,
            vol_download,
            vol_games,
            vol_voip,
            vol_web,
            vol_others,
            vol_2G,
            vol_3G,
            vol_LTE,
            CASE WHEN vol_streaming = GREATEST(vol_streaming, vol_chat, vol_download, vol_games, vol_others, vol_voip, vol_web) THEN 'Streaming'
                 WHEN vol_chat = GREATEST(vol_streaming, vol_chat, vol_download, vol_games, vol_others, vol_voip, vol_web) THEN 'Chat'
                 WHEN vol_download = GREATEST(vol_streaming, vol_chat, vol_download, vol_games, vol_others, vol_voip, vol_web) THEN 'Download'
                 WHEN vol_games = GREATEST(vol_streaming, vol_chat, vol_download, vol_games, vol_others, vol_voip, vol_web) THEN 'Games'
                 WHEN vol_voip = GREATEST(vol_streaming, vol_chat, vol_download, vol_games, vol_others, vol_voip, vol_web) THEN 'VoIP'
                 WHEN vol_web = GREATEST(vol_streaming, vol_chat, vol_download, vol_games, vol_others, vol_voip, vol_web) THEN 'Web'
                 ELSE 'Others'
                 END AS contenu_pref,
            GREATEST(vol_streaming, vol_chat, vol_download, vol_games, vol_others, vol_voip, vol_web) AS vol_contenu_pref,
            GREATEST(vol_2G, vol_3G, vol_LTE) AS vol_techno_pref,
            CASE WHEN (vol_2G > vol_3G AND vol_2G > vol_LTE) THEN '2G'
                 WHEN (vol_3G > vol_LTE AND vol_3G > vol_2G) THEN '3G'
                 ELSE 'LTE'
                 END AS techno_pref  
        FROM 
        (
            SELECT 
                imei,
                SUM(vol_streaming) AS vol_streaming,
                SUM(vol_chat) AS vol_chat,
                SUM(vol_download) AS vol_download,
                SUM(vol_games) AS vol_games,
                SUM(vol_voip) AS vol_voip,
                SUM(vol_web) AS vol_web,
                SUM(vol_others) AS vol_others,
                SUM(vol_2G) AS vol_2G,
                SUM(vol_3G) AS vol_3G,
                SUM(vol_LTE) AS vol_LTE
            FROM
            (
                SELECT 
                    imei,
                    msisdn,
                    tac_code
                FROM MON.SPARK_FT_IMEI_CBM
                WHERE transaction_date = '###SLICE_VALUE###'
            ) D11 
            LEFT JOIN 
            (
                SELECT 
                    msisdn,
                    imei AS tac_code,
                    SUM(nbytest * (CASE WHEN appli_type = 'Streaming' THEN 1 ELSE 0 END)) AS vol_streaming,
                    SUM(nbytest * (CASE WHEN appli_type = 'Web' THEN 1 ELSE 0 END)) AS vol_web,
                    SUM(nbytest * (CASE WHEN appli_type = 'Download' THEN 1 ELSE 0 END)) AS vol_download,
                    SUM(nbytest * (CASE WHEN appli_type = 'Games' THEN 1 ELSE 0 END)) AS vol_games,
                    SUM(nbytest * (CASE WHEN appli_type = 'Chat' THEN 1 ELSE 0 END)) AS vol_chat,
                    SUM(nbytest * (CASE WHEN appli_type = 'VoIP' THEN 1 ELSE 0 END)) AS vol_voip,
                    SUM(nbytest * (CASE WHEN appli_type NOT IN ('Streaming', 'Web', 'Download', 'Games', 'Chat', 'VoIP') THEN 1 ELSE 0 END)) AS vol_others,
                    SUM(nbytest * (CASE WHEN radio_access_techno = '2G' THEN 1 ELSE 0 END)) AS vol_2G,
                    SUM(nbytest * (CASE WHEN radio_access_techno = '3G' THEN 1 ELSE 0 END)) AS vol_3G,
                    SUM(nbytest * (CASE WHEN radio_access_techno = 'LTE' THEN 1 ELSE 0 END)) AS vol_LTE
                FROM MON.SPARK_FT_OTARIE_DATA_TRAFFIC_DAY
                WHERE transaction_date = '###SLICE_VALUE###'
                GROUP BY msisdn, imei
            ) D12
            ON D11.msisdn = D12.msisdn
            AND D11.tac_code = D12.tac_code
            GROUP BY imei
        ) D1
    ) D
    ON A.imei = D.imei
    LEFT JOIN
    (
        SELECT DISTINCT
            imei,
            CASE WHEN typebox = 'MF293N1' THEN 'MF293N'
                 ELSE typebox
                 END AS typebox
        FROM DMC_BI.BOX_EN_STOCK
    ) E 
    ON A.imei = E.imei
    WHERE (typebox IS NULL) OR (typebox != 'AIRBOX') 