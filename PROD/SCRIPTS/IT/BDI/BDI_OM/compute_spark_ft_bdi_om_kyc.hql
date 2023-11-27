INSERT INTO MON.SPARK_FT_BDI_OM_KYC
SELECT 
    iban,
    nom_naiss,
    nom_marital,
    prenom,
    sexe,
    date_naissance,
    pays_naissance,
    lieu_naissance,
    nom_prenom_mere,
    nom_prenom_pere,
    nationalite,
    profession_client,
    nom_tutelle,
    statut_client,
    situation_bancaire,
    situation_judiciaire,
    date_debut_interdiction_judiciaire,
    date_fin_interdiction_judiciaire,
    type_piece_identite,
    piece_identite,
    date_emission_piece_identification,
    date_fin_validite,
    lieu_emission_piece,
    pays_emission_piece,
    pays_residence,
    code_agent_economique,
    code_secteur_activite,
    rrc,
    ppe,
    risque_AML,
    pro,
    guid,
    msisdn,
    type_compte,
    acceptation_cgu,
    contrat_soucription,
    date_validation,
    date_creaction_compte,
    disponibilite_scan,
    identificateur,
    motif_rejet_bo,
    statut_validation_bo,
    date_maj_om,
    imei,
    region_administrative,
    ville,
    nature_client_titulaire_compte,
    numero_compte,
    (case when iban is not null and trim(iban) <> '' and numero_compte is not null and trim(numero_compte) <> '' and nom_naiss is not null and trim(nom_naiss) <> '' and 
    prenom is not null and trim(prenom) <> '' and sexe is not null and trim(sexe) <> '' and date_naissance is not null and trim(date_naissance) <> '' and pays_naissance is not null and 
    trim(pays_naissance) <> '' and lieu_naissance is not null and trim(lieu_naissance) <> '' and nationalite is not null and trim(nationalite) <> '' and profession_client is not null and 
    trim(profession_client) <> '' and type_piece_identite is not null and trim(type_piece_identite) <> '' and piece_identite is not null and trim(piece_identite) <> '' and 
    date_fin_validite is not null and trim(date_fin_validite) <> '' and lieu_emission_piece is not null and trim(lieu_emission_piece) <> '' and acceptation_cgu is not null and 
    trim(acceptation_cgu) <> '' and contrat_soucription is not null and trim(contrat_soucription) <> '' and disponibilite_scan is not null and trim(disponibilite_scan) <> '' 
    and ville is not null and trim(ville) <> '' then 'OUI' else 'NON' end) est_conforme_art,
    EST_ACTIF_30J,
    EST_ACTIF_90J,
    est_multicompte_om,
    est_client_telco,
    est_suspendu_telco,
    est_suspendu_om,
    (case when iban is null or trim(iban) = '' then 'OUI' else 'NON' end) iban_absent,
    (case when nom_naiss is null or trim(nom_naiss) = '' then 'OUI' else 'NON' end) nom_naiss_absent,
    (case when nom_marital is null or trim(nom_marital) = '' then 'OUI' else 'NON' end) nom_marital_absent,
    (case when prenom is null or trim(prenom) = '' then 'OUI' else 'NON' end) prenom_absent,
    (case when sexe is null or trim(sexe) = '' then 'OUI' else 'NON' end) sexe_absent,
    (case when date_naissance is null or trim(date_naissance) = '' then 'OUI' else 'NON' end) date_naissance_absent,
    (case when pays_naissance is null or trim(pays_naissance) = '' then 'OUI' else 'NON' end) pays_naissance_absent,
    (case when lieu_naissance is null or trim(lieu_naissance) = '' then 'OUI' else 'NON' end) lieu_naissance_absent,
    (case when nom_prenom_mere is null or trim(nom_prenom_mere) = '' then 'OUI' else 'NON' end) nom_prenom_mere_absent,
    (case when nom_prenom_pere is null or trim(nom_prenom_pere) = '' then 'OUI' else 'NON' end) nom_prenom_pere_absent,
    (case when nationalite is null or trim(nationalite) = '' then 'OUI' else 'NON' end) nationalite_absent,
    (case when profession_client is null or trim(profession_client) = '' then 'OUI' else 'NON' end) profession_client_absent,
    (case when nom_tutelle is null or trim(nom_tutelle) = '' then 'OUI' else 'NON' end) nom_tutelle_absent,
    (case when statut_client is null or trim(statut_client) = '' then 'OUI' else 'NON' end) statut_client_absent,
    (case when situation_bancaire is null or trim(situation_bancaire) = '' then 'OUI' else 'NON' end) situation_bancaire_absent,
    (case when situation_judiciaire is null or trim(situation_judiciaire) = '' then 'OUI' else 'NON' end) situation_judiciaire_absent,
    (case when date_debut_interdiction_judiciaire is null or trim(date_debut_interdiction_judiciaire) = '' then 'OUI' else 'NON' end) date_debut_interdiction_judiciaire_absent,
    (case when date_fin_interdiction_judiciaire is null or trim(date_fin_interdiction_judiciaire) = '' then 'OUI' else 'NON' end) date_fin_interdiction_judiciaire_absent,
    (case when type_piece_identite is null or trim(type_piece_identite) = '' then 'OUI' else 'NON' end) type_piece_identite_absent,
    (case when piece_identite is null or trim(piece_identite) = '' then 'OUI' else 'NON' end) piece_identite_absent,
    (case when date_emission_piece_identification is null or trim(date_emission_piece_identification) = '' then 'OUI' else 'NON' end) date_emission_piece_identification_absent,
    (case when date_fin_validite is null or trim(date_fin_validite) = '' then 'OUI' else 'NON' end) date_fin_validite_absent,
    (case when lieu_emission_piece is null or trim(lieu_emission_piece) = '' then 'OUI' else 'NON' end) lieu_emission_piece_absent,
    (case when pays_emission_piece is null or trim(pays_emission_piece) = '' then 'OUI' else 'NON' end) pays_emission_piece_absent,
    event_date
FROM (SELECT * FROM TMP.TT_BDI_OM_KYC_1 ) A
LEFT JOIN (SELECT * FROM  TMP.TT_BDI_OM_KYC_2) B
ON A.piece_identite = B.numeropiece