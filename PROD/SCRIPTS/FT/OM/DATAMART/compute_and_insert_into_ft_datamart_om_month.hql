INSERT INTO MON.FT_DATAMART_OM_MONTH
        SELECT
            END_ACS.MSISDN,
            END_ACS.USER_ID,
            TO_DATE(END_ACS.CREATION_DATE) DATE_CREATION_CPTE,
            NVL(ACD.DATE_DERNIERE_ACTIVITE_OM, END_ACS.LAST_TRANSFER_ON) DATE_DERNIERE_ACTIVITE_OM,
            NVL(ACD.NB_JR_ACTIVITE, 0) NB_JR_ACTIVITE,
            NVL(ACD.NB_OPERATIONS, 0) NB_OPERATIONS,
            NVL(ACD.NB_SERVICES_DISTINCTS, 0) NB_SERVICES_DISTINCTS,
            NVL(START_ACS.ACCOUNT_BALANCE, 0) SOLDE_DEBUT_MOIS,
            NVL(END_ACS.ACCOUNT_BALANCE, 0) SOLDE_FIN_MOIS,
            NVL(ACD.ARPU_OM, 0)ARPU_OM ,
            NVL(ACD.NB_CI, 0) NB_CI,
            NVL(ACD.MONTANT_CI, 0) MONTANT_CI,
            NVL(ACD.NB_CO, 0) NB_CO,
            NVL(ACD.MONTANT_CO, 0) MONTANT_CO,
            NVL(ACD.FRAIS_CO, 0) FRAIS_CO,
            NVL(ACD.NB_BILL_PAY, 0) NB_BILL_PAY,
            NVL(ACD.MONTANT_BILL_PAY, 0) MONTANT_BILL_PAY,
            NVL(ACD.FRAIS_BILL_PAY, 0) FRAIS_BILL_PAY,
            NVL(ACD.NB_MERCHPAY, 0) NB_MERCHPAY,
            NVL(ACD.MONTANT_MERCHPAY, 0) MONTANT_MERCHPAY,
            NVL(ACD.FRAIS_MERCHPAY, 0) FRAIS_MERCHPAY,
            NVL(ACD.NB_PARTENAIRES_DISTINCTS, 0) NB_PARTENAIRES_DISTINCTS,
            NVL(ACD.NB_P2P_RECU, 0) NB_P2P_RECU,
            NVL(ACD.MONTANT_P2P_RECU, 0) MONTANT_P2P_RECU,
            NVL(ACD.NB_MSISDN_TRANSMIS_P2P, 0) NB_MSISDN_TRANSMIS_P2P,
            NVL(ACD.NB_P2P_ORANGE, 0) NB_P2P_ORANGE,
            NVL(ACD.MONTANT_P2P_ORANGE, 0) MONTANT_P2P_ORANGE,
            NVL(ACD.FRAIS_P2P_ORANGE, 0) FRAIS_P2P_ORANGE,
            NVL(ACD.NB_TNO, 0) NB_TNO,
            NVL(ACD.MONTANT_TNO, 0) MONTANT_TNO,
            NVL(ACD.FRAIS_TNO, 0) FRAIS_TNO,
            NVL(ACD.NB_MSISDN_RECUS_P2P, 0) NB_MSISDN_RECUS_P2P,
            NVL(ACD.NB_TOTAL_P2P, 0) NB_TOTAL_P2P,
            NVL(ACD.MONTANT_TOTAL_P2P, 0) MONTANT_TOTAL_P2P,
            NVL(ACD.FRAIS_TOTAL_P2P, 0) FRAIS_TOTAL_P2P,
            NVL(ACD.NB_TOP_UP, 0) NB_TOP_UP,
            NVL(ACD.MONTANT_TOP_UP, 0) MONTANT_TOP_UP,
            NVL(ACD.NB_TOP_UP_POUR_TIER, 0) NB_TOP_UP_POUR_TIER,
            NVL(ACD.MONTANT_TOP_UP_POUR_TIER, 0) MONTANT_TOP_UP_POUR_TIER,
            NVL(ACD.NB_AUTRES, 0) NB_AUTRES,
            NVL(ACD.MONTANT_AUTRES, 0) MONTANT_AUTRES,
            NVL(ACD.NB_BUNDLES_DATA, 0) NB_BUNDLES_DATA,
            NVL(ACD.MONTANT_BDLE_DATA, 0) MONTANT_BDLE_DATA,
            NVL(ACD.NB_BUNDLE_VOIX, 0) NB_BUNDLE_VOIX,
            NVL(ACD.MONTANT_BDLE_VOIX, 0) MONTANT_BDLE_VOIX,
            END_ACS.USER_TYPE,
            CURRENT_TIMESTAMP INSERT_DATE,
			NVL(ACD.MOIS,date_format(END_ACS.EVENT_DATE,'yyyy-MM')) MOIS
        FROM 
        (
            SELECT
                FT.EVENT_DATE,
                FT.USER_ID,
                UPPER(FT.USER_TYPE) USER_TYPE,
                FT.MSISDN,
                FT.CREATION_DATE,
                FT.ACCOUNT_BALANCE,
                IT.LAST_TRANSFER_ON
            FROM 
            (
                SELECT *
                FROM
                (
                    SELECT A.*,                 
                    ROW_NUMBER() OVER (PARTITION BY USER_ID ORDER BY MODIFIED_ON DESC)  AS RANG
                    FROM MON.FT_OMNY_ACCOUNT_SNAPSHOT A
                    WHERE EVENT_DATE = last_day(concat('2019-12','-01'))
                ) FT1 WHERE RANG = 1
            ) FT
            LEFT JOIN
            (
                SELECT DISTINCT
                USER_ID,
                MODIFIED_ON,
                LAST_TRANSFER_ON
                FROM
                (
                SELECT
                USER_ID,
                MODIFIED_ON MODIFIED_ON,
                LAST_TRANSFER_ON,
                ROW_NUMBER() OVER (PARTITION BY USER_ID ORDER BY MODIFIED_ON DESC)  AS RANG
                FROM CDR.IT_OMNY_ACCOUNT_SNAPSHOT
                WHERE ORIGINAL_FILE_DATE = DATE_ADD(last_day(concat('2019-12','-01')),1)
                ) IT1
                WHERE RANG=1
            ) IT ON (FT.USER_ID = IT.USER_ID)
            WHERE MSISDN IS NOT NULL
        ) END_ACS
        LEFT JOIN
        (
            SELECT *
            FROM MON.FT_OMNY_ACCOUNT_SNAPSHOT
            WHERE EVENT_DATE = CONCAT('2019-12','-01')
        ) START_ACS ON (END_ACS.USER_ID = START_ACS.USER_ID)
        LEFT JOIN (
            SELECT
                A.MSISDN,
                A.USER_ID,
                A.MOIS,
                C.NB_JR_ACTIVITE,
                A.NB_OPERATIONS,
                B.NB_SERVICES_DISTINCTS,
                D.DATE_DERNIERE_ACTIVITE_OM,
                A.ARPU_OM,
                A.NB_CI,
                A.MONTANT_CI,
                A.NB_CO,
                A.MONTANT_CO,
                A.FRAIS_CO,
                A.NB_BILL_PAY,
                A.MONTANT_BILL_PAY,
                A.FRAIS_BILL_PAY,
                A.NB_MERCHPAY,
                A.MONTANT_MERCHPAY,
                A.FRAIS_MERCHPAY,
                A.NB_PARTENAIRES_DISTINCTS,
                A.NB_P2P_RECU,
                A.MONTANT_P2P_RECU,
                A.NB_MSISDN_TRANSMIS_P2P,
                A.NB_P2P_ORANGE,
                A.MONTANT_P2P_ORANGE,
                A.FRAIS_P2P_ORANGE,
                A.NB_TNO,
                A.MONTANT_TNO,
                A.FRAIS_TNO,
                A.NB_MSISDN_RECUS_P2P,
                A.NB_TOTAL_P2P,
                A.MONTANT_TOTAL_P2P,
                A.FRAIS_TOTAL_P2P,
                A.NB_TOP_UP,
                A.MONTANT_TOP_UP,
                A.NB_TOP_UP_POUR_TIER,
                A.MONTANT_TOP_UP_POUR_TIER,
                A.NB_AUTRES,
                A.MONTANT_AUTRES,
                A.NB_BUNDLES_DATA,
                A.MONTANT_BDLE_DATA,
                A.NB_BUNDLE_VOIX,
                A.MONTANT_BDLE_VOIX
            FROM
            (
                SELECT
                    A.MSISDN,
                    A.USER_ID,
                    A.MOIS,
                    SUM(A.NB_OPERATIONS) NB_OPERATIONS,
                    SUM(A.ARPU_OM) ARPU_OM,
                    SUM(A.NB_CI) NB_CI,
                    SUM(A.MONTANT_CI) MONTANT_CI,
                    SUM(A.NB_CO) NB_CO,
                    SUM(A.MONTANT_CO) MONTANT_CO,
                    SUM(A.FRAIS_CO) FRAIS_CO,
                    SUM(A.NB_BILL_PAY) NB_BILL_PAY,
                    SUM(A.MONTANT_BILL_PAY) MONTANT_BILL_PAY,
                    SUM(A.FRAIS_BILL_PAY) FRAIS_BILL_PAY,
                    SUM(A.NB_MERCHPAY) NB_MERCHPAY,
                    SUM(A.MONTANT_MERCHPAY) MONTANT_MERCHPAY,
                    SUM(A.FRAIS_MERCHPAY) FRAIS_MERCHPAY,
                    SUM(A.NB_PARTENAIRES_DISTINCTS) NB_PARTENAIRES_DISTINCTS,
                    SUM(A.NB_P2P_RECU) NB_P2P_RECU,
                    SUM(A.MONTANT_P2P_RECU) MONTANT_P2P_RECU,
                    SUM(A.NB_MSISDN_TRANSMIS_P2P) NB_MSISDN_TRANSMIS_P2P,
                    SUM(A.NB_P2P_ORANGE) NB_P2P_ORANGE,
                    SUM(A.MONTANT_P2P_ORANGE) MONTANT_P2P_ORANGE,
                    SUM(A.FRAIS_P2P_ORANGE) FRAIS_P2P_ORANGE,
                    SUM(A.NB_TNO) NB_TNO,
                    SUM(A.MONTANT_TNO) MONTANT_TNO,
                    SUM(A.FRAIS_TNO) FRAIS_TNO,
                    SUM(A.NB_MSISDN_RECUS_P2P) NB_MSISDN_RECUS_P2P,
                    SUM(A.NB_TOTAL_P2P) NB_TOTAL_P2P,
                    SUM(A.MONTANT_TOTAL_P2P) MONTANT_TOTAL_P2P,
                    SUM(A.FRAIS_TOTAL_P2P) FRAIS_TOTAL_P2P,
                    SUM(A.NB_TOP_UP) NB_TOP_UP,
                    SUM(A.MONTANT_TOP_UP) MONTANT_TOP_UP,
                    SUM(A.NB_TOP_UP_POUR_TIER) NB_TOP_UP_POUR_TIER,
                    SUM(A.MONTANT_TOP_UP_POUR_TIER) MONTANT_TOP_UP_POUR_TIER,
                    SUM(A.NB_AUTRES) NB_AUTRES,
                    SUM(A.MONTANT_AUTRES) MONTANT_AUTRES,
                    SUM(A.NB_BUNDLES_DATA) NB_BUNDLES_DATA,
                    SUM(A.MONTANT_BDLE_DATA) MONTANT_BDLE_DATA,
                    SUM(A.NB_BUNDLE_VOIX) NB_BUNDLE_VOIX,
                    SUM(A.MONTANT_BDLE_VOIX) MONTANT_BDLE_VOIX
                FROM
                (
                    SELECT
                        OT.SENDER_MSISDN MSISDN,
                        OT.SENDER_USER_ID USER_ID,


                        COUNT(*) NB_OPERATIONS,
                        SUM(OT.SERVICE_CHARGE_RECEIVED) ARPU_OM,
                        0 NB_CI,
                        0 MONTANT_CI,
                        SUM(CASE WHEN OT.SERVICE_TYPE = 'CASHOUT' THEN 1 ELSE 0 END) NB_CO,
                        SUM(CASE WHEN OT.SERVICE_TYPE = 'CASHOUT' THEN OT.TRANSACTION_AMOUNT ELSE 0 END) MONTANT_CO,
                        SUM(CASE WHEN OT.SERVICE_TYPE = 'CASHOUT' THEN OT.SERVICE_CHARGE_RECEIVED ELSE 0 END) FRAIS_CO,
                        SUM(CASE WHEN OT.SERVICE_TYPE = 'BILLPAY' THEN 1 ELSE 0 END) NB_BILL_PAY,
                        SUM(CASE WHEN OT.SERVICE_TYPE = 'BILLPAY' THEN OT.TRANSACTION_AMOUNT ELSE 0 END) MONTANT_BILL_PAY,
                        SUM(CASE WHEN OT.SERVICE_TYPE = 'BILLPAY' THEN OT.SERVICE_CHARGE_RECEIVED ELSE 0 END) FRAIS_BILL_PAY,
                        SUM(CASE WHEN OT.SERVICE_TYPE = 'MERCHPAY' THEN 1 ELSE 0 END) NB_MERCHPAY,
                        SUM(CASE WHEN OT.SERVICE_TYPE = 'MERCHPAY' THEN OT.TRANSACTION_AMOUNT ELSE 0 END) MONTANT_MERCHPAY,
                        SUM(CASE WHEN OT.SERVICE_TYPE = 'MERCHPAY' THEN OT.SERVICE_CHARGE_RECEIVED ELSE 0 END) FRAIS_MERCHPAY,
                        COUNT(DISTINCT (CASE WHEN OT.SERVICE_TYPE = 'MERCHPAY' THEN OT.RECEIVER_MSISDN ELSE NULL END)) NB_PARTENAIRES_DISTINCTS,
                        0 MONTANT_P2P_RECU,
                        0 NB_P2P_RECU,
                        0 NB_MSISDN_TRANSMIS_P2P,
                        SUM(CASE WHEN OT.SERVICE_TYPE = 'P2P' THEN 1 ELSE 0 END) NB_P2P_ORANGE,
                        SUM(CASE WHEN OT.SERVICE_TYPE = 'P2P' THEN OT.TRANSACTION_AMOUNT ELSE 0 END) MONTANT_P2P_ORANGE,
                        SUM(CASE WHEN OT.SERVICE_TYPE = 'P2P' THEN OT.SERVICE_CHARGE_RECEIVED ELSE 0 END) FRAIS_P2P_ORANGE,
                        SUM(CASE WHEN OT.SERVICE_TYPE = 'P2PNONREG' THEN 1 ELSE 0 END) NB_TNO,
                        SUM(CASE WHEN OT.SERVICE_TYPE = 'P2PNONREG' THEN OT.TRANSACTION_AMOUNT ELSE 0 END) MONTANT_TNO,
                        SUM(CASE WHEN OT.SERVICE_TYPE = 'P2PNONREG' THEN OT.SERVICE_CHARGE_RECEIVED ELSE 0 END) FRAIS_TNO,
                        COUNT(DISTINCT (CASE WHEN OT.SERVICE_TYPE IN ('P2P', 'P2PNONREG') THEN OT.RECEIVER_MSISDN ELSE NULL END)) NB_MSISDN_RECUS_P2P,
                        SUM(CASE WHEN OT.SERVICE_TYPE IN ('P2P', 'P2PNONREG') THEN 1 ELSE 0 END) NB_TOTAL_P2P,
                        SUM(CASE WHEN OT.SERVICE_TYPE IN ('P2P', 'P2PNONREG') THEN OT.TRANSACTION_AMOUNT ELSE 0 END) MONTANT_TOTAL_P2P,
                        SUM(CASE WHEN OT.SERVICE_TYPE IN ('P2P', 'P2PNONREG') THEN OT.SERVICE_CHARGE_RECEIVED ELSE 0 END) FRAIS_TOTAL_P2P,
                        SUM(CASE WHEN OT.TRANSACTION_TAG = 'TOP UP' THEN 1 ELSE 0 END) NB_TOP_UP,
                        SUM(CASE WHEN OT.TRANSACTION_TAG = 'TOP UP' THEN OT.TRANSACTION_AMOUNT ELSE 0 END) MONTANT_TOP_UP,
                        SUM(CASE WHEN OT.TRANSACTION_TAG = 'TOP UP' AND OT.SENDER_MSISDN <> OT.OTHER_MSISDN THEN 1 ELSE 0 END) NB_TOP_UP_POUR_TIER,
                        SUM(CASE WHEN OT.TRANSACTION_TAG = 'TOP UP' AND OT.SENDER_MSISDN <> OT.OTHER_MSISDN THEN OT.TRANSACTION_AMOUNT ELSE 0 END) MONTANT_TOP_UP_POUR_TIER,
                        SUM(CASE WHEN OT.SERVICE_TYPE NOT IN ('CASHOUT', 'BILLPAY', 'MERCHPAY', 'P2P', 'P2PNONREG') AND OT.TRANSACTION_TAG <> 'TOP UP' THEN 1 ELSE 0 END) NB_AUTRES,
                        SUM(CASE WHEN OT.SERVICE_TYPE NOT IN ('CASHOUT', 'BILLPAY', 'MERCHPAY', 'P2P', 'P2PNONREG') AND OT.TRANSACTION_TAG <> 'TOP UP' THEN OT.TRANSACTION_AMOUNT ELSE 0 END) MONTANT_AUTRES,
                        SUM(CASE WHEN RECEIVER_MSISDN = '698066666' AND OT.SERVICE_TYPE = 'MERCHPAY' THEN 1 ELSE 0 END) NB_BUNDLES_DATA,
                        SUM(CASE WHEN RECEIVER_MSISDN = '698066666' AND OT.SERVICE_TYPE = 'MERCHPAY' THEN OT.TRANSACTION_AMOUNT ELSE 0 END) MONTANT_BDLE_DATA,
                        0 NB_BUNDLE_VOIX,
                        0 MONTANT_BDLE_VOIX,
                        date_format(OT.TRANSFER_DATETIME,'yyyy-MM') MOIS
                    FROM CDR.IT_OMNY_TRANSACTIONS OT
                    WHERE TRANSFER_DATETIME between concat('2019-12','-01') and last_day(concat('2019-12','-01'))
                    AND OT.TRANSFER_DONE LIKE '%Y%'
                    GROUP BY OT.SENDER_MSISDN, OT.SENDER_USER_ID , date_format(OT.TRANSFER_DATETIME,'yyyy-MM')
                    UNION ALL
                    SELECT
                        OT.RECEIVER_MSISDN MSISDN,
                        OT.RECEIVER_USER_ID USER_ID,


                        COUNT(*) NB_OPERATIONS,
                        SUM(OT.SERVICE_CHARGE_RECEIVED) ARPU_OM,
                        SUM(CASE WHEN OT.SERVICE_TYPE = 'CASHIN' THEN 1 ELSE 0 END) NB_CI,
                        SUM(CASE WHEN OT.SERVICE_TYPE = 'CASHIN' THEN OT.TRANSACTION_AMOUNT ELSE 0 END) MONTANT_CI,
                        0 NB_CO,
                        0 MONTANT_CO,
                        0 FRAIS_CO,
                        0 NB_BILL_PAY,
                        0 MONTANT_BILL_PAY,
                        0 FRAIS_BILL_PAY,
                        0 NB_MERCHPAY,
                        0 MONTANT_MERCHPAY,
                        0 FRAIS_MERCHPAY,
                        0 NB_PARTENAIRES_DISTINCTS,
                        SUM(CASE WHEN OT.SERVICE_TYPE = 'P2P' THEN OT.TRANSACTION_AMOUNT ELSE 0 END) MONTANT_P2P_RECU,
                        SUM(CASE WHEN OT.SERVICE_TYPE = 'P2P' THEN 1 ELSE 0 END) NB_P2P_RECU,
                        COUNT(DISTINCT (CASE WHEN OT.SERVICE_TYPE = 'P2P' THEN OT.SENDER_MSISDN ELSE NULL END)) NB_MSISDN_TRANSMIS_P2P,
                        0 NB_P2P_ORANGE,
                        0 MONTANT_P2P_ORANGE,
                        0 FRAIS_P2P_ORANGE,
                        0 NB_TNO,
                        0 MONTANT_TNO,
                        0 FRAIS_TNO,
                        0 NB_MSISDN_RECUS_P2P,
                        0 NB_TOTAL_P2P,
                        0 MONTANT_TOTAL_P2P,
                        0 FRAIS_TOTAL_P2P,
                        0 NB_TOP_UP,
                        0 MONTANT_TOP_UP,
                        0 NB_TOP_UP_POUR_TIER,
                        0 MONTANT_TOP_UP_POUR_TIER,
                        SUM(CASE WHEN OT.SERVICE_TYPE NOT IN ('CASHOUT', 'BILLPAY', 'MERCHPAY', 'P2P', 'P2PNONREG') AND OT.TRANSACTION_TAG <> 'TOP UP' THEN 1 ELSE 0 END) NB_AUTRES,
                        SUM(CASE WHEN OT.SERVICE_TYPE NOT IN ('CASHOUT', 'BILLPAY', 'MERCHPAY', 'P2P', 'P2PNONREG') AND OT.TRANSACTION_TAG <> 'TOP UP' THEN OT.TRANSACTION_AMOUNT ELSE 0 END) MONTANT_AUTRES,
                        0 NB_BUNDLES_DATA,
                        0 MONTANT_BDLE_DATA,
                        0 NB_BUNDLE_VOIX,
                        0 MONTANT_BDLE_VOIX,
                        date_format(OT.TRANSFER_DATETIME,'yyyy-MM')  MOIS
                    FROM CDR.IT_OMNY_TRANSACTIONS OT
                    WHERE TRANSFER_DATETIME between concat('2019-12','-01') and last_day(concat('2019-12','-01'))
                    AND OT.TRANSFER_DONE LIKE '%Y%'
                    GROUP BY OT.RECEIVER_MSISDN, OT.RECEIVER_USER_ID, date_format(OT.TRANSFER_DATETIME,'yyyy-MM')
                ) A
                GROUP BY A.MSISDN, A.USER_ID, A.MOIS
            ) A
            LEFT JOIN
            (
                SELECT
                   A1.USER_ID,
                   COUNT(DISTINCT A1.SERVICE_TYPE) NB_SERVICES_DISTINCTS
                FROM
                (
                    SELECT
                        OT.SENDER_USER_ID USER_ID,
                        OT.SERVICE_TYPE
                    FROM CDR.IT_OMNY_TRANSACTIONS OT
                    WHERE TRANSFER_DATETIME between concat('2019-12','-01') and last_day(concat('2019-12','-01'))
                    AND OT.TRANSFER_DONE LIKE '%Y%'
                    UNION ALL
                    SELECT
                        OT.RECEIVER_USER_ID USER_ID,
                        OT.SERVICE_TYPE
                    FROM CDR.IT_OMNY_TRANSACTIONS OT
                    WHERE TRANSFER_DATETIME BETWEEN  concat('2019-12','-01') and last_day(concat('2019-12','-01'))
                    AND OT.TRANSFER_DONE LIKE '%Y%'
                ) A1
                GROUP BY A1.USER_ID
            ) B ON (A.USER_ID = B.USER_ID)
            LEFT JOIN
            (
                SELECT
                    A2.USER_ID USER_ID,
                    COUNT(DISTINCT TRANSACTION_DATE) NB_JR_ACTIVITE
                FROM
                (
                    SELECT
                        OT.SENDER_USER_ID USER_ID,
                        TO_DATE(OT.TRANSFER_DATETIME) TRANSACTION_DATE
                    FROM CDR.IT_OMNY_TRANSACTIONS OT
                    WHERE TRANSFER_DATETIME between concat('2019-12','-01') and last_day(concat('2019-12','-01'))
                    AND OT.TRANSFER_DONE LIKE '%Y%'
                    UNION ALL
                    SELECT
                        OT.RECEIVER_USER_ID USER_ID,
                        TO_DATE(OT.TRANSFER_DATETIME) TRANSACTION_DATE
                    FROM CDR.IT_OMNY_TRANSACTIONS OT
                    WHERE TRANSFER_DATETIME between concat('2019-12','-01') and last_day(concat('2019-12','-01'))
                    AND OT.TRANSFER_DONE LIKE '%Y%'
                ) A2
                GROUP BY A2.USER_ID
            ) C ON (C.USER_ID = A.USER_ID)
            LEFT JOIN
            (
                SELECT
                   A1.USER_ID,
                   MAX(TO_DATE(TRANSFER_DATETIME)) DATE_DERNIERE_ACTIVITE_OM
                FROM
                (
                    SELECT
                        OT.SENDER_USER_ID USER_ID,
                        MAX(TO_DATE(OT.TRANSFER_DATETIME)) TRANSFER_DATETIME
                    FROM CDR.IT_OMNY_TRANSACTIONS OT
                    WHERE TRANSFER_DATETIME between concat('2019-12','-01') and last_day(concat('2019-12','-01'))
                    AND OT.TRANSFER_DONE LIKE '%Y%'
                    GROUP BY OT.SENDER_USER_ID
                    UNION ALL
                    SELECT
                        OT.RECEIVER_USER_ID USER_ID,
                        MAX(TO_DATE(OT.TRANSFER_DATETIME)) TRANSFER_DATETIME
                    FROM CDR.IT_OMNY_TRANSACTIONS OT
                    WHERE TRANSFER_DATETIME BETWEEN  concat('2019-12','-01') and last_day(concat('2019-12','-01'))
                    AND OT.TRANSFER_DONE LIKE '%Y%'
                    GROUP BY OT.RECEIVER_USER_ID
                ) A1
                GROUP BY A1.USER_ID
            ) D ON (D.USER_ID = A.USER_ID)
        ) ACD ON (ACD.MSISDN = END_ACS.MSISDN AND ACD.USER_ID = END_ACS.USER_ID);