INSERT INTO TMP.TT_BDI_OM_KYA_STEP_5

SELECT 
    nvl(B.piece_identite,A.piece_identite) as piece_identite,
    case when B.iban is null or trim(B.iban) = '' then A.iban else B.iban end as iban,
    case when B.rib is null or trim(B.rib) = '' then A.rib else B.rib end as rib,
    case when B.ibu is null or trim(B.ibu) = '' then A.ibu else B.ibu end as ibu,
    nvl(B.numero_compte,A.numero_compte) as numero_compte,
    case when B.nature_compte is null or trim(B.nature_compte) = '' then A.nature_compte else B.nature_compte end as nature_compte,
    case when B.id_complete_mandataire is null or trim(B.id_complete_mandataire) = '' then A.id_complete_mandataire else B.id_complete_mandataire end as id_complete_mandataire,
    case when B.date_creation is null or trim(B.date_creation) = '' then A.date_creation else B.date_creation end as date_creation,
    case when B.duree_de_vie is null or trim(B.duree_de_vie) = '' then A.duree_de_vie else B.duree_de_vie end as duree_de_vie,
    case when B.raison_sociale is null or trim(B.raison_sociale) = '' then A.raison_sociale else B.raison_sociale end as raison_sociale,
    case when B.sigle_raison_sociale is null or trim(B.sigle_raison_sociale) = '' then A.sigle_raison_sociale else B.sigle_raison_sociale end as sigle_raison_sociale,
    case when B.forme_juridique is null or trim(B.forme_juridique) = '' then A.forme_juridique else B.forme_juridique end as forme_juridique,
    case when B.secteur_activite_eco is null or trim(B.secteur_activite_eco) = '' then A.secteur_activite_eco else B.secteur_activite_eco end as secteur_activite_eco,
    case when B.numero_regis_commerce is null or trim(B.numero_regis_commerce) = '' then A.numero_regis_commerce else B.numero_regis_commerce end as numero_regis_commerce,
    case when B.numero_identification_fiscale is null or trim(B.numero_identification_fiscale) = '' then A.numero_identification_fiscale else B.numero_identification_fiscale end as numero_identification_fiscale,
    case when B.pays_siege_social is null or trim(B.pays_siege_social) = '' then A.pays_siege_social else B.pays_siege_social end as pays_siege_social,
    case when B.ville_siege_social is null or trim(B.ville_siege_social) = '' then A.ville_siege_social else B.ville_siege_social end as ville_siege_social,
    case when B.statut_client is null or trim(B.statut_client) = '' then A.statut_client else B.statut_client end as statut_client,
    case when B.nature_client_titulaire_compte is null or trim(B.nature_client_titulaire_compte) = '' then A.nature_client_titulaire_compte else B.nature_client_titulaire_compte end as nature_client_titulaire_compte,
    case when B.type_compte is null or trim(B.type_compte) = '' then A.type_compte else B.type_compte end as type_compte,
    case when B.code_devise is null or trim(B.code_devise) = '' then A.code_devise else B.code_devise end as code_devise,
    case when B.statut_compte is null or trim(B.statut_compte) = '' then A.statut_compte else B.statut_compte end as statut_compte,
    case when B.id_interne_mandataire is null or trim(B.id_interne_mandataire) = '' then A.id_interne_mandataire else B.id_interne_mandataire end as id_interne_mandataire,
    case when B.numero_piece_mandataire is null or trim(B.numero_piece_mandataire) = '' then A.numero_piece_mandataire else B.numero_piece_mandataire end as numero_piece_mandataire,
    case when B.numero_compte_om_mandataire is null or trim(B.numero_compte_om_mandataire) = '' then A.numero_compte_om_mandataire else B.numero_compte_om_mandataire end as numero_compte_om_mandataire,
    case when B.numero_ibu_mandataire is null or trim(B.numero_ibu_mandataire) = '' then A.numero_ibu_mandataire else B.numero_ibu_mandataire end as numero_ibu_mandataire,
    case when B.situation_judiciaire is null or trim(B.situation_judiciaire) = '' then A.situation_judiciaire else B.situation_judiciaire end as situation_judiciaire,
    case when B.date_debut_interdiction_judiciaire is null or trim(B.date_debut_interdiction_judiciaire) = '' then A.date_debut_interdiction_judiciaire else B.date_debut_interdiction_judiciaire end as date_debut_interdiction_judiciaire,
    case when B.date_fin_interdiction_judiciaire is null or trim(B.date_fin_interdiction_judiciaire) = '' then A.date_fin_interdiction_judiciaire else B.date_fin_interdiction_judiciaire end as date_fin_interdiction_judiciaire,
    case when B.service_souscrit is null or trim(B.service_souscrit) = '' then A.service_souscrit else B.service_souscrit end as service_souscrit,
    nvl(B.guid,A.guid) as guid,
    case when B.pays_residence is null or trim(B.pays_residence) = '' then A.pays_residence else B.pays_residence end as pays_residence,
    case when B.code_agent_economique is null or trim(B.code_agent_economique) = '' then A.code_agent_economique else B.code_agent_economique end as code_agent_economique,
    case when B.code_secteur_activite is null or trim(B.code_secteur_activite) = '' then A.code_secteur_activite else B.code_secteur_activite end as code_secteur_activite,
    case when B.notation_interne is null or trim(B.notation_interne) = '' then A.notation_interne else B.notation_interne end as notation_interne,
    case when B.ppe is null or trim(B.ppe) = '' then A.ppe else B.ppe end as ppe,
    case when B.risque_AML is null or trim(B.risque_AML) = '' then A.risque_AML else B.risque_AML end as risque_AML,
    case when B.groupe is null or trim(B.groupe) = '' then A.groupe else B.groupe end as groupe,
    case when B.profil_interne is null or trim(B.profil_interne) = '' then A.profil_interne else B.profil_interne end as profil_interne,
    case when B.qualite_mandataire is null or trim(B.qualite_mandataire) = '' then A.qualite_mandataire else B.qualite_mandataire end as qualite_mandataire,
    case when B.responsabilite_compte is null or trim(B.responsabilite_compte) = '' then A.responsabilite_compte else B.responsabilite_compte end as responsabilite_compte,
    case when B.date_expiration_piece_mandataire is null or trim(B.date_expiration_piece_mandataire) = '' then A.date_expiration_piece_mandataire else B.date_expiration_piece_mandataire end as date_expiration_piece_mandataire,
    case when B.lieu_emission_piece_mandataire is null or trim(B.lieu_emission_piece_mandataire) = '' then A.lieu_emission_piece_mandataire else B.lieu_emission_piece_mandataire end as lieu_emission_piece_mandataire,
    case when B.nom_mandataire is null or trim(B.nom_mandataire) = '' then A.nom_mandataire else B.nom_mandataire end as nom_mandataire,
    case when B.prenom_mandataire is null or trim(B.prenom_mandataire) = '' then A.prenom_mandataire else B.prenom_mandataire end as prenom_mandataire,
    case when B.nationalite_mandataire is null or trim(B.nationalite_mandataire) = '' then A.nationalite_mandataire else B.nationalite_mandataire end as nationalite_mandataire,
    case when B.date_naissance_mandataire is null or trim(B.date_naissance_mandataire) = '' then A.date_naissance_mandataire else B.date_naissance_mandataire end as date_naissance_mandataire,
    case when B.est_multicompte_om is null or trim(B.est_multicompte_om) = '' then A.est_multicompte_om else B.est_multicompte_om end as est_multicompte_om,
    case when B.est_actif_30 is null or trim(B.est_actif_30) = '' then A.est_actif_30 else B.est_actif_30 end as est_actif_30,
    case when B.est_actif_90 is null or trim(B.est_actif_90) = '' then A.est_actif_90 else B.est_actif_90 end as est_actif_90,
    case when B.est_client_telco is null or trim(B.est_client_telco) = '' then A.est_client_telco else B.est_client_telco end as est_client_telco,
    case when B.est_suspendu_telco is null or trim(B.est_suspendu_telco) = '' then A.est_suspendu_telco else B.est_suspendu_telco end as est_suspendu_telco,
    case when B.est_suspendu_om is null or trim(B.est_suspendu_om) = '' then A.est_suspendu_om else B.est_suspendu_om end as est_suspendu_om,
    B.event_date
FROM (SELECT * FROM MON.SPARK_FT_BDI_OM_KYA WHERE EVENT_DATE=date_sub('###SLICE_VALUE###',1) ) A
FULL OUTER JOIN
    (SELECT 
    piece_identite,
    iban,
    rib,
    ibu,
    numero_compte,
    nature_compte,
    id_complete_mandataire,
    date_creation,
    duree_de_vie,
    raison_sociale,
    sigle_raison_sociale,
    forme_juridique,
    secteur_activite_eco,
    numero_regis_commerce,
    numero_identification_fiscale,
    pays_siege_social,
    ville_siege_social,
    statut_client,
    nature_client_titulaire_compte,
    type_compte,
    code_devise,
    statut_compte,
    id_interne_mandataire,
    numero_piece_mandataire,
    numero_compte_om_mandataire,
    numero_ibu_mandataire,
    situation_judiciaire,
    date_debut_interdiction_judiciaire,
    date_fin_interdiction_judiciaire,
    service_souscrit,
    guid,
    pays_residence,
    code_agent_economique,
    code_secteur_activite,
    notation_interne,
    ppe,
    risque_AML,
    groupe,
    profil_interne,
    qualite_mandataire,
    responsabilite_compte,
    date_expiration_piece_mandataire,
    lieu_emission_piece_mandataire,
    nom_mandataire,
    prenom_mandataire,
    nationalite_mandataire,
    date_naissance_mandataire,
    est_multicompte_om,
    est_actif_30,
    est_actif_90,
    est_client_telco,
    est_suspendu_om,
    est_suspendu_telco,
    event_date
    FROM (SELECT * FROM TMP.TT_BDI_OM_KYA_STEP_3 ) AA
    LEFT JOIN (SELECT * FROM  TMP.TT_BDI_OM_KYA_STEP_4) AAA
    ON AA.piece_identite = AAA.numeropiece)B
    ON  trim(A.numero_compte) = trim(B.numero_compte)