SELECT
    msisdn,
    user_id,
    FROM_UNIXTIME(UNIX_TIMESTAMP(PERIOD, 'yyyy-MM-dd'), 'dd/MM/yyyy') period,
    nb_operations,
    nb_services_distincts,
    solde_fin_journee,
    arpu_om,
    nb_ci,
    montant_ci,
    nb_co,
    montant_co,
    frais_co,
    nb_bill_pay,
    montant_bill_pay,
    frais_bill_pay,
    nb_merchpay,
    montant_merchpay,
    frais_merchpay,
    montant_p2p_recu,
    nb_p2p_recu,
    montant_p2p_orange,
    frais_p2p_orange,
    montant_tno,
    frais_tno,
    nb_top_up,
    montant_top_up,
    nb_autres,
    montant_autres,
    nb_bundles_data,
    montant_bdle_data,
    nb_bundle_voix,
    montant_bdle_voix,
    category_code,
    domain_code,
    grade_name
FROM MON.SPARK_FT_DATAMART_OM_DAILY
WHERE PERIOD = '###SLICE_VALUE###'