insert into CDR.SPARK_IT_BDI_ART
select
it1.msisdn ,
it1.type_personne ,
case when (it1.nom_prenom_absent_ft = 'OUI' or it1.nom_prenom_douteux_ft = 'OUI') and zsm.nom_prenom_absent = 'NON' then zsm.nom_prenom
     when (it1.nom_prenom_absent_ft = 'OUI' or it1.nom_prenom_douteux_ft = 'OUI') and om.nom_prenom_douteux = 'NON' then om.nom_prenom
     when (it1.nom_prenom_absent_ft = 'OUI' or it1.nom_prenom_douteux_ft = 'OUI') and omid.nom_prenom_douteux = 'NON' then omid.nom_prenom
     else it1.nom_prenom_ft
end as nom_prenom,
it1.id_type_piece ,
it1.type_piece ,
case when (it1.numero_piece_absent_ft = 'OUI' or it1.numero_piece_non_authorise_ft = 'OUI'
        or it1.numero_piece_uniquement_lettre_ft = 'OUI' or
           it1.numero_piece_inf_4_ft ='OUI' or
            it1.numero_piece_a_caract_non_auth_ft = 'OUI' or
            it1.numero_piece_egale_msisdn_ft = 'OUI') and zsm.numero_piece_absent = 'NON' then zsm.numero_piece
     when  (it1.numero_piece_absent_ft = 'OUI' or it1.numero_piece_non_authorise_ft = 'OUI'
        or it1.numero_piece_uniquement_lettre_ft = 'OUI' or
           it1.numero_piece_inf_4_ft ='OUI' or
            it1.numero_piece_a_caract_non_auth_ft = 'OUI' or
            it1.numero_piece_egale_msisdn_ft = 'OUI')  and om.numero_piece_absent = 'NON' then om.numero_piece
     when  (it1.numero_piece_absent_ft = 'OUI' or it1.numero_piece_non_authorise_ft = 'OUI'
        or it1.numero_piece_uniquement_lettre_ft = 'OUI' or
           it1.numero_piece_inf_4_ft ='OUI' or
            it1.numero_piece_a_caract_non_auth_ft = 'OUI' or
            it1.numero_piece_egale_msisdn_ft = 'OUI')  and omid.numero_piece_absent  = 'NON' then omid.numero_piece
     else it1.numero_piece_ft
end as numero_piece,
case when (it1.date_expiration_absent_ft = 'OUI' or it1.date_expiration_douteuse_ft = 'OUI'
           ) and zsm.DATE_EXPIRATION_ABSENTE = 'NON' then cast(zsm.DATE_EXPIRATION as string)
     when (it1.date_expiration_absent_ft = 'OUI' or it1.date_expiration_douteuse_ft = 'OUI'
           ) and omid.DATE_EXPIRATION_ABSENTE = 'NON' then cast(omid.dateexpire as string)
     else it1.date_expiration
end date_expiration ,
case when (it1.date_naissance_absent_ft = 'OUI' or it1.date_naissance_douteux_ft  = 'OUI')
            and zsm.DATE_NAISSANCE_ABSENT = 'NON' then cast(zsm.DATE_NAISSANCE as string)
     when (it1.date_naissance_absent_ft = 'OUI' or it1.date_naissance_douteux_ft  = 'OUI')
            and om.date_naissance_absent = 'NON' then cast(om.date_naissance as string)
    when (it1.date_naissance_absent_ft = 'OUI' or it1.date_naissance_douteux_ft  = 'OUI')
          and omid.DATE_NAISSANCE_ABSENT =  'NON' then cast(omid.DATE_NAISSANCE as string)
    else it1.date_naissance
end  date_naissance ,
case when it1.date_activation_ft is null and zsm.date_activation is not null then cast(zsm.date_activation  as string)
     when it1.date_activation_ft is null and ft_csnap.activation_date is not null then cast(ft_csnap.activation_date as string)
     else it1.date_activation
end as date_activation,
case when not(ft_clsd.adresse is null or ft_clsd.adresse = '') then ft_clsd.adresse
     else it1.addresse
end as addresse,
it1.quartier ,
it1.ville ,
it1.statut_bscs ,
it1.statut_validation_bo ,
it1.motif_rejet_bo ,
it1.date_validation_bo ,
it1.login_validateur_bo ,
it1.canal_validateur_bo ,
it1.type_abonnement ,
it1.csmoddate ,
it1.ccmoddate ,
it1.compte_client_structure ,
it1.nom_structure ,
it1.numero_registre_commerce ,
it1.numero_piece_rep_legal ,
it1.imei ,
it1.statut_derogation ,
it1.region_administrative ,
it1.region_commerciale ,
it1.site_name ,
it1.ville_site ,
it1.offre_commerciale ,
it1.type_contrat ,
it1.segmentation ,
it1.score_vip ,
it1.date_souscription,
it1.date_changement_statut ,
it1.ville_structure ,
it1.quartier_structure ,
it1.raison_statut ,
case when (it1.prenom_ft is null or trim(it1.prenom_ft) = '') and not(zsm.prenom is null or trim(zsm.prenom)='') then zsm.prenom
     when (it1.prenom_ft is null or trim(it1.prenom_ft) = '') and not(om.prenom is null or trim(om.prenom) = '' ) then om.prenom
     when (it1.prenom_ft is null or trim(it1.prenom_ft) = '') and not(omid.prenom is null or trim(omid.prenom) = '' ) then omid.prenom
     else it1.prenom
end as prenom,
case when (it1.nom_ft is null or trim(it1.nom_ft) = '') and not(zsm.nom is null or trim(zsm.nom)='') then zsm.nom
     when (it1.nom_ft is null or trim(it1.nom_ft) = '') and not(om.nom is null or trim(om.nom)='') then om.nom
     when (it1.nom_ft is null or trim(it1.nom_ft) = '') and not(omid.nom is null or trim(omid.nom)='') then omid.nom
     else it1.nom
end as nom,
it1.customer_id ,
it1.contract_id ,
it1.compte_client ,
it1.plan_localisation ,
case when (it1.contrat_soucription_ft is null or trim(it1.contrat_soucription_ft) = '') and
        not(zsm.contrat_soucription is null or trim(zsm.contrat_soucription) = '')  then trim(zsm.contrat_soucription)
     when (it1.contrat_soucription_ft is null or trim(it1.contrat_soucription_ft) = '') and
        (zsm.contrat_soucription is null or trim(zsm.contrat_soucription) = '')
          then 'data11/' || IF(it1.MSISDN is null or trim(it1.MSISDN) = '','200',SUBSTR(trim(it1.MSISDN),7,3)) || '/' || nvl(trim(it1.msisdn),'690009200') || '.jpeg'
end AS CONTRAT_SOUCRIPTION,
it1.acceptation_cgv_ft ,
it1.disponibilite_scan ,
case when (it1.nom_parent_absent_ft = 'OUI' or it1.nom_parent_douteux_ft = 'OUI')
        and not(zsm.nom_parent is null or trim(zsm.nom_parent ) = '')  then trim(zsm.nom_parent)
    else it1.nom_parent_ft
end AS nom_parent,
case when  (it1.prenom_tuteur is null or trim(it1.prenom_tuteur) = '')
        and not(zsm.prenom_tuteur is null or zsm.prenom_tuteur = '')  then trim(zsm.PRENOM_TUTEUR)
     else it1.prenom_tuteur
end AS PRENOM_TUTEUR ,
case when (it1.date_naissance_tut_absent_ft = 'OUI' or it1.date_naissance_tut_douteux_ft = 'OUI')
           and zsm.DATE_NAISSANCE_TUT_absent = 'NON' then cast(zsm.DATE_NAISSANCE_TUTEUR as string)
     else it1.date_naissance_tuteur
end AS DATE_NAISSANCE_TUTEUR,
case when (it1.numero_piece_tut_absent_ft = 'OUI' or numero_piece_tut_non_auth_ft = 'OUI'
    or it1.numero_piece_tut_egale_msisdn_ft = 'OUI') and not(zsm.NUMERO_PIECE_TUTEUR is null or trim(zsm.NUMERO_PIECE_TUTEUR) = '')
        then zsm.NUMERO_PIECE_TUTEUR
    else it1.NUMERO_PIECE_TUTEUR
end AS NUMERO_PIECE_TUTEUR,
case when (it1.date_expiration_tuteur is null or trim(it1.date_expiration_tuteur) = '')
            and not(zsm.date_expiration_tuteur is null or trim(zsm.date_expiration_tuteur) = '')
                then zsm.date_expiration_tuteur
     ELSE it1.date_expiration_tuteur
end as date_expiration_tuteur,
it1.id_type_piece_tuteur ,
it1.type_piece_tuteur ,
it1.adresse_tuteur ,
it1.identificateur ,
it1.localisation_identificateur ,
it1.profession ,
it1.odbincomingcalls ,
it1.odboutgoingcalls ,
current_timestamp() AS insert_date,
it1.ORIGINAL_FILE_DATE
from (
select a.*,b.*
from
(select *
from CDR.SPARK_IT_BDI where ORIGINAL_FILE_DATE=to_date('2020-05-11')) a
join (select
msisdn AS msisdn_ft,
type_piece AS type_piece_ft,
numero_piece AS numero_piece_ft,
nom_prenom AS nom_prenom_ft,
nom AS nom_ft,
prenom AS prenom_ft,
date_naissance AS date_naissance_ft,
date_expiration AS date_expiration_ft,
adresse AS adresse_ft,
numero_piece_tuteur AS numero_piece_tuteur_ft,
nom_parent AS nom_parent_ft,
date_naissance_tuteur AS date_naissance_tuteur_ft,
nom_structure AS nom_structure_ft,
numero_registre_commerce AS numero_registre_commerce_ft,
numero_piece_rep_legal AS numero_piece_rep_legal_ft,
date_activation AS date_activation_ft,
date_changement_statut AS date_changement_statut_ft,
statut_bscs AS statut_bscs_ft,
odbincomingcalls AS odbincomingcalls_ft,
odboutgoingcalls AS odboutgoingcalls_ft,
imei AS imei_ft,
statut_derogation AS statut_derogation_ft,
region_administrative AS region_administrative_ft,
region_commerciale AS region_commerciale_ft,
site_name AS site_name_ft,
ville AS ville_ft,
longitude AS longitude_ft,
latitude AS latitude_ft,
offre_commerciale AS offre_commerciale_ft,
type_contrat AS type_contrat_ft,
segmentation AS segmentation_ft,
rev_m_3 AS rev_m_3_ft,
rev_m_2 AS rev_m_2_ft,
rev_m_1 AS rev_m_1_ft,
rev_moy AS rev_moy_ft,
statut_in AS statut_in_ft,
numero_piece_absent AS numero_piece_absent_ft,
numero_piece_tut_absent AS numero_piece_tut_absent_ft,
numero_piece_inf_4 AS numero_piece_inf_4_ft,
numero_piece_tut_inf_4 AS numero_piece_tut_inf_4_ft,
numero_piece_non_authorise AS numero_piece_non_authorise_ft,
numero_piece_tut_non_auth AS numero_piece_tut_non_auth_ft,
numero_piece_egale_msisdn AS numero_piece_egale_msisdn_ft,
numero_piece_tut_egale_msisdn AS numero_piece_tut_egale_msisdn_ft,
numero_piece_a_caract_non_auth AS numero_piece_a_caract_non_auth_ft,
numero_piece_tut_carac_non_a AS numero_piece_tut_carac_non_a_ft,
numero_piece_uniquement_lettre AS numero_piece_uniquement_lettre_ft,
numero_piece_tut_uniq_lettre AS numero_piece_tut_uniq_lettre_ft,
nom_prenom_absent AS nom_prenom_absent_ft,
nom_parent_absent AS nom_parent_absent_ft,
nom_prenom_douteux AS nom_prenom_douteux_ft,
nom_parent_douteux AS nom_parent_douteux_ft,
date_naissance_absent AS date_naissance_absent_ft,
date_naissance_tut_absent AS date_naissance_tut_absent_ft,
date_expiration_absent AS date_expiration_absent_ft,
adresse_absent AS adresse_absent_ft,
adresse_douteuse AS adresse_douteuse_ft,
type_personne_inconnu AS type_personne_inconnu_ft,
mineur_mal_identifie AS mineur_mal_identifie_ft,
type_personne AS type_personne_ft,
date_acquisition AS date_acquisition_ft,
date_naissance_douteux AS date_naissance_douteux_ft,
date_naissance_tut_douteux AS date_naissance_tut_douteux_ft,
date_expiration_douteuse AS date_expiration_douteuse_ft,
cni_expire AS cni_expire_ft,
multi_sim AS multi_sim_ft,
est_present_om AS est_present_om_ft,
est_present_zeb AS est_present_zeb_ft,
est_present_art AS est_present_art_ft,
est_present_gp AS est_present_gp_ft,
est_present_ocm AS est_present_ocm_ft,
est_actif_om AS est_actif_om_ft,
est_client_vip AS est_client_vip_ft,
rev_om_m_3 AS rev_om_m_3_ft,
rev_om_m_2 AS rev_om_m_2_ft,
rev_om_m_1 AS rev_om_m_1_ft,
est_actif_data AS est_actif_data_ft,
traffic_data_m_3 AS traffic_data_m_3_ft,
traffic_data_m_2 AS traffic_data_m_2_ft,
traffic_data_m_1 AS traffic_data_m_1_ft,
conform_ocm_p_morale_m2m AS conform_ocm_p_morale_m2m_ft,
conform_art_p_morale_m2m AS conform_art_p_morale_m2m_ft,
conform_ocm_p_morale_flotte AS conform_ocm_p_morale_flotte_ft,
conform_art_p_morale_flotte AS conform_art_p_morale_flotte_ft,
conform_ocm_p_phy_majeur AS conform_ocm_p_phy_majeur_ft,
conform_art_p_phy_majeur AS conform_art_p_phy_majeur_ft,
conform_ocm_p_phy_mineur AS conform_ocm_p_phy_mineur_ft,
conform_art_p_phy_mineur AS conform_art_p_phy_mineur_ft,
est_suspendu AS est_suspendu_ft,
nom_structure_absent AS nom_structure_absent_ft,
numero_registre_absent AS numero_registre_absent_ft,
numero_registre_douteux AS numero_registre_douteux_ft,
conforme_art AS conforme_art_ft,
conforme_ocm AS conforme_ocm_ft,
imei_absent AS imei_absent_ft,
est_premium AS est_premium_ft,
adresse_tuteur AS adresse_tuteur_ft,
type_piece_tuteur AS type_piece_tuteur_ft,
acceptation_cgv AS acceptation_cgv_ft,
contrat_soucription AS contrat_soucription_ft,
disponibilite_scan AS disponibilite_scan_ft,
plan_localisation AS plan_localisation_ft,
identificateur AS identificateur_ft,
profession_identificateur AS profession_identificateur_ft,
date_validation_bo AS date_validation_bo_ft,
statut_validation_bo AS statut_validation_bo_ft,
motif_rejet_bo AS motif_rejet_bo_ft,
statut_validation_boo AS statut_validation_boo_ft,
disponibilite_scan_sid AS disponibilite_scan_sid_ft,
est_conforme_maj_kyc AS est_conforme_maj_kyc_ft,
est_conforme_min_kyc AS est_conforme_min_kyc_ft,
est_snappe AS est_snappe_ft,
insert_date AS insert_date_ft,
event_date AS event_date_ft
from Mon.spark_ft_bdi where event_date=to_date('2020-05-10')) b
on substr(trim(a.msisdn),-9,9) = substr(trim(b.msisdn_ft),-9,9)
) it1
left join (
select *
from TMP.TT_OM_PHOTO_CONFORMITE
) om on substr(trim(it1.msisdn),-9,9) = substr(trim(om.msisdn),-9,9)
left join (
select * from TMP.TT_ZSMART_CONFORMITE
) zsm on substr(trim(it1.msisdn),-9,9) = substr(trim(zsm.msisdn),-9,9)
left join (
select aa.access_key,aa.activation_date
from (
select  access_key,activation_date,
row_number() over(partition by access_key order by activation_date desc nulls last) as rang
FROM MON.SPARK_FT_CONTRACT_SNAPSHOT
WHERE EVENT_DATE = to_date('2020-05-11')
) aa where rang = 1
) ft_csnap on substr(trim(it1.msisdn),-9,9) = substr(trim(ft_csnap.access_key),-9,9)
left join (
SELECT
msisdn,
nvl(trim(TOWNNAME),'') || ' ' || nvl(trim(site_name),'') as adresse
FROM (
SELECT a.*, ROW_NUMBER() OVER (PARTITION  BY msisdn ORDER BY LAST_LOCATION_DAY DESC, insert_date desc) RN
FROM MON.SPARK_FT_CLIENT_LAST_SITE_DAY a
WHERE EVENT_DATE = to_date('2020-05-11')
) x WHERE RN=1
) ft_clsd  on substr(trim(it1.msisdn),-9,9) = substr(trim(ft_clsd.msisdn),-9,9)
left join (
select * from TMP.TT_MYOMID_CONFORMITE
) omid  on substr(trim(it1.msisdn),-9,9) = substr(trim(omid.msisdn),-9,9)