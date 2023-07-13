INSERT INTO MON.SPARK_FT_DATAMART_OM_DAILY

SELECT

    A.MSISDN,
    A.USER_ID,
    A.NB_OPERATIONS,
    A.NB_SERVICES_DISTINCTS,
    B.ACCOUNT_BALANCE SOLDE_FIN_JOURNEE,
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
    A.MONTANT_P2P_RECU,
    A.NB_P2P_RECU,
    A.MONTANT_P2P_ORANGE,
    A.FRAIS_P2P_ORANGE,
    A.MONTANT_TNO,
    A.FRAIS_TNO,
    A.NB_TOP_UP,
    A.MONTANT_TOP_UP,
    A.NB_AUTRES,
    A.MONTANT_AUTRES,
    A.NB_BUNDLES_DATA,
    A.MONTANT_BDLE_DATA,
    A.NB_BUNDLE_VOIX,
    A.MONTANT_BDLE_VOIX,
    CURRENT_TIMESTAMP INSERT_DATE,
    category_code,
    domain_code,
    grade_name,
    A.PERIOD




FROM
(
    SELECT
        A.MSISDN,
        A.USER_ID,
        SUM(A.NB_OPERATIONS) NB_OPERATIONS,
        COUNT(DISTINCT A.SERVICE_TYPE) NB_SERVICES_DISTINCTS,
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
        SUM(A.MONTANT_P2P_RECU) MONTANT_P2P_RECU,
        SUM(A.NB_P2P_RECU) NB_P2P_RECU,
        SUM(A.MONTANT_P2P_ORANGE) MONTANT_P2P_ORANGE,
        SUM(A.FRAIS_P2P_ORANGE) FRAIS_P2P_ORANGE,
        SUM(A.MONTANT_TNO) MONTANT_TNO,
        SUM(A.FRAIS_TNO) FRAIS_TNO,
        SUM(A.NB_TOP_UP) NB_TOP_UP,
        SUM(A.MONTANT_TOP_UP) MONTANT_TOP_UP,
        SUM(A.NB_AUTRES) NB_AUTRES,
        SUM(A.MONTANT_AUTRES) MONTANT_AUTRES,
        SUM(A.NB_BUNDLES_DATA) NB_BUNDLES_DATA,
        SUM(A.MONTANT_BDLE_DATA) MONTANT_BDLE_DATA,
        SUM(A.NB_BUNDLE_VOIX) NB_BUNDLE_VOIX,
        SUM(A.MONTANT_BDLE_VOIX) MONTANT_BDLE_VOIX,
        A.PERIOD,
        category_code,
        domain_code,
        grade_name

    FROM
    (
        SELECT
            OT.SENDER_MSISDN MSISDN,
            OT.SENDER_USER_ID USER_ID,
            COUNT(*) NB_OPERATIONS,
            OT.SERVICE_TYPE,
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
            0 MONTANT_P2P_RECU,
            0 NB_P2P_RECU,
            SUM(CASE WHEN OT.SERVICE_TYPE = 'P2P' THEN OT.TRANSACTION_AMOUNT ELSE 0 END) MONTANT_P2P_ORANGE,
            SUM(CASE WHEN OT.SERVICE_TYPE = 'P2P' THEN OT.SERVICE_CHARGE_RECEIVED ELSE 0 END) FRAIS_P2P_ORANGE,
            SUM(CASE WHEN OT.SERVICE_TYPE = 'P2PNONREG' THEN OT.TRANSACTION_AMOUNT ELSE 0 END) MONTANT_TNO,
            SUM(CASE WHEN OT.SERVICE_TYPE = 'P2PNONREG' THEN OT.SERVICE_CHARGE_RECEIVED ELSE 0 END) FRAIS_TNO,
            SUM(CASE WHEN OT.TRANSACTION_TAG = 'TOP UP' THEN 1 ELSE 0 END) NB_TOP_UP,
            SUM(CASE WHEN OT.TRANSACTION_TAG = 'TOP UP' THEN OT.TRANSACTION_AMOUNT ELSE 0 END) MONTANT_TOP_UP,
            SUM(CASE WHEN OT.SERVICE_TYPE NOT IN ('CASHOUT', 'BILLPAY', 'MERCHPAY', 'P2P', 'P2PNONREG') AND OT.TRANSACTION_TAG <> 'TOP UP' THEN 1 ELSE 0 END) NB_AUTRES,
            SUM(CASE WHEN OT.SERVICE_TYPE NOT IN ('CASHOUT', 'BILLPAY', 'MERCHPAY', 'P2P', 'P2PNONREG') AND OT.TRANSACTION_TAG <> 'TOP UP' THEN OT.TRANSACTION_AMOUNT ELSE 0 END) MONTANT_AUTRES,
            SUM(CASE WHEN RECEIVER_MSISDN in ('691202290','691203011','691210767','691211294','691211300','691211384','691211407','691211426','691211457','691212249','691212442','691300807','691633320','691724958','691017480','691724967','691961236','691972383','691013822','691673659','697578544','655578850','691568039','696518674','656974923','655987452','695574764','695109901','693264943','698869548','656330712','656963493','698505275','656966808','656589950','697259779','655574506','655575025','655415494','655574762','656812877','656313094','656933711','693423524','656079791','697768586','698000338','656292871','695133960','695952904','656927123','655989215','698454763','656425506','694554270','655864487','656941109','698370593','656313556','695497312','698975730','656204130','655610673','655780847','656131945','655533935','693411825','699729740','693632072','656289146','693358501','695841143','693301789','656181836','699814962','656675448','656833485','693242352','656864091','699086407','694923436','656940781','656172832','695325647','698066666','658101010','658121212','691724895','693801855','691110998','692036620','692051908','692051896','692050772','692051906','692050771','692050743','692050744','691300986','691300913','691300916','691300888','691300902','692052024','692052043','692052045','692052046','692051548','692052056','692052093','692052098','692052112','692052113','692052119','692052137','692052138','692052159','692052172','692053554','692053514','692053515','692053527','692053524','692053533','692053553','692053562','692053564','692053578','692251953','692228083','692015704','692000911','692000988','655844658','656907599','695846041','655578102','656230098','694056170','656300836','696844033','698161416') AND OT.SERVICE_TYPE = 'MERCHPAY' THEN 1 ELSE 0 END) NB_BUNDLES_DATA,
            SUM(CASE WHEN RECEIVER_MSISDN in ('691202290','691203011','691210767','691211294','691211300','691211384','691211407','691211426','691211457','691212249','691212442','691300807','691633320','691724958','691017480','691724967','691961236','691972383','691013822','691673659','697578544','655578850','691568039','696518674','656974923','655987452','695574764','695109901','693264943','698869548','656330712','656963493','698505275','656966808','656589950','697259779','655574506','655575025','655415494','655574762','656812877','656313094','656933711','693423524','656079791','697768586','698000338','656292871','695133960','695952904','656927123','655989215','698454763','656425506','694554270','655864487','656941109','698370593','656313556','695497312','698975730','656204130','655610673','655780847','656131945','655533935','693411825','699729740','693632072','656289146','693358501','695841143','693301789','656181836','699814962','656675448','656833485','693242352','656864091','699086407','694923436','656940781','656172832','695325647','698066666','658101010','658121212','691724895','693801855','691110998','692036620','692051908','692051896','692050772','692051906','692050771','692050743','692050744','691300986','691300913','691300916','691300888','691300902','692052024','692052043','692052045','692052046','692051548','692052056','692052093','692052098','692052112','692052113','692052119','692052137','692052138','692052159','692052172','692053554','692053514','692053515','692053527','692053524','692053533','692053553','692053562','692053564','692053578','692251953','692228083','692015704','692000911','692000988','655844658','656907599','695846041','655578102','656230098','694056170','656300836','696844033','698161416') AND OT.SERVICE_TYPE = 'MERCHPAY' THEN OT.TRANSACTION_AMOUNT ELSE 0 END) MONTANT_BDLE_DATA,
            0 NB_BUNDLE_VOIX,
            0 MONTANT_BDLE_VOIX,
            TO_DATE(OT.TRANSFER_DATETIME) PERIOD,
            sender_category_code category_code,
            sender_domain_code domain_code,
            sender_grade_name grade_name

        FROM CDR.SPARK_IT_OMNY_TRANSACTIONS  OT
        WHERE TRANSFER_DATETIME = '###SLICE_VALUE###' AND OT.TRANSFER_DONE LIKE '%Y%'
        GROUP BY SENDER_MSISDN, SENDER_USER_ID, OT.SERVICE_TYPE, TO_DATE(OT.TRANSFER_DATETIME), sender_category_code, sender_domain_code, sender_grade_name
        UNION ALL
        SELECT
            OT.RECEIVER_MSISDN MSISDN,
            OT.RECEIVER_USER_ID USER_ID,
            COUNT(*) NB_OPERATIONS,
            OT.SERVICE_TYPE,
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
            SUM(CASE WHEN OT.SERVICE_TYPE = 'P2P' THEN OT.TRANSACTION_AMOUNT ELSE 0 END) MONTANT_P2P_RECU,
            SUM(CASE WHEN OT.SERVICE_TYPE = 'P2P' THEN 1 ELSE 0 END) NB_P2P_RECU,
            0 MONTANT_P2P_ORANGE,
            0 FRAIS_P2P_ORANGE,
            0 MONTANT_TNO,
            0 FRAIS_TNO,
            0 NB_TOP_UP,
            0 MONTANT_TOP_UP,
            SUM(CASE WHEN OT.SERVICE_TYPE NOT IN ('CASHIN', 'P2P') THEN 1 ELSE 0 END) NB_AUTRES,
            SUM(CASE WHEN OT.SERVICE_TYPE NOT IN ('CASHIN', 'P2P') THEN OT.TRANSACTION_AMOUNT ELSE 0 END) MONTANT_AUTRES,
            0 NB_BUNDLES_DATA,
            0 MONTANT_BDLE_DATA,
            0 NB_BUNDLE_VOIX,
            0 MONTANT_BDLE_VOIX,
            TO_DATE(OT.TRANSFER_DATETIME) PERIOD,
            receiver_category_code category_code,
            receiver_domain_code domain_code,
            receiver_grade_name grade_name

        FROM CDR.SPARK_IT_OMNY_TRANSACTIONS  OT
        WHERE TRANSFER_DATETIME = '###SLICE_VALUE###' AND OT.TRANSFER_DONE LIKE '%Y%'
        GROUP BY OT.RECEIVER_MSISDN, OT.RECEIVER_USER_ID, OT.SERVICE_TYPE, TO_DATE(OT.TRANSFER_DATETIME), receiver_category_code, receiver_domain_code, receiver_grade_name
    ) A
GROUP BY A.MSISDN, A.USER_ID, A.PERIOD, category_code, domain_code, grade_name
) A

LEFT JOIN 

(SELECT * FROM MON.SPARK_FT_OMNY_ACCOUNT_SNAPSHOT WHERE EVENT_DATE = '###SLICE_VALUE###') B 

ON A.USER_ID = B.USER_ID



