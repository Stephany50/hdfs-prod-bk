SELECT
    MSISDN,
    USER_ID,
    MOIS,
    DATE_CREATION_CPTE,
    DATE_DERNIERE_ACTIVITE_OM,
    NB_JR_ACTIVITE,
    NB_OPERATIONS,
    NB_SERVICES_DISTINCTS,
    SOLDE_DEBUT_MOIS,
    SOLDE_FIN_MOIS,
    ARPU_OM,
    NB_CI,
    MONTANT_CI,
    NB_CO,
    MONTANT_CO,
    FRAIS_CO,
    NB_BILL_PAY,
    MONTANT_BILL_PAY,
    FRAIS_BILL_PAY,
    NB_MERCHPAY,
    MONTANT_MERCHPAY,
    FRAIS_MERCHPAY,
    NB_PARTENAIRES_DISTINCTS,
    NB_P2P_RECU,
    MONTANT_P2P_RECU,
    NB_MSISDN_TRANSMIS_P2P,
    NB_P2P_ORANGE,
    MONTANT_P2P_ORANGE,
    FRAIS_P2P_ORANGE,
    NB_TNO,
    MONTANT_TNO,
    FRAIS_TNO,
    NB_MSISDN_RECUS_P2P,
    NB_TOTAL_P2P,
    MONTANT_TOTAL_P2P,
    FRAIS_TOTAL_P2P,
    NB_TOP_UP,
    MONTANT_TOP_UP,
    NB_TOP_UP_POUR_TIER,
    MONTANT_TOP_UP_POUR_TIER,
    NB_AUTRES,
    MONTANT_AUTRES,
    NB_BUNDLES_DATA,
    MONTANT_BDLE_DATA,
    NB_BUNDLE_VOIX,
    MONTANT_BDLE_VOIX,
    USER_TYPE
FROM MON.SPARK_FT_DATAMART_OM_MONTH
WHERE MOIS = '###SLICE_VALUE###'