select
    a.*,
    user_domain_code domain_code,
    user_category_code category_code,
    user_grade_name grade_name
from
(
    SELECT
        msisdn,
        user_id,
        mois,
        date_creation_cpte,
        date_derniere_activite_om,
        nb_jr_activite,
        nb_operations,
        nb_services_distincts,
        solde_debut_mois,
        solde_fin_mois,
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
        nb_partenaires_distincts,
        nb_p2p_recu,
        montant_p2p_recu,
        nb_msisdn_transmis_p2p,
        nb_p2p_orange,
        montant_p2p_orange,
        frais_p2p_orange,
        nb_tno,
        montant_tno,
        frais_tno,
        nb_msisdn_recus_p2p,
        nb_total_p2p,
        montant_total_p2p,
        frais_total_p2p,
        nb_top_up,
        montant_top_up,
        nb_top_up_pour_tier,
        montant_top_up_pour_tier,
        nb_autres,
        montant_autres,
        nb_bundles_data,
        montant_bdle_data,
        nb_bundle_voix,
        montant_bdle_voix,
        user_type
    FROM MON.SPARK_FT_DATAMART_OM_MONTH
    WHERE MOIS = '###SLICE_VALUE###'
) a
left join
(
    select
        msisdn,
        user_domain_code,
        user_category_code,
        user_grade_name
    from cdr.spark_it_om_all_users
    where original_file_date = last_day('###SLICE_VALUE###'||'-1')
) c on a.msisdn = c.msisdn