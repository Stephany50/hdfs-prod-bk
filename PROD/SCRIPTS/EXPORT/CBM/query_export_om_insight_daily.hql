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
        montant_bdle_voix
    FROM MON.SPARK_FT_DATAMART_OM_DAILY
    WHERE PERIOD = '###SLICE_VALUE###'
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