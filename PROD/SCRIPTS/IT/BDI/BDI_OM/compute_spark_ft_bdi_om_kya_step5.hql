INSERT INTO TMP.TT_BDI_OM_KYA_STEP_5
SELECT 
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
(CASE WHEN iban is not null and trim(iban) <> '' AND numero_compte is not null and trim(numero_compte) <> '' AND raison_sociale is not null and trim(raison_sociale) <> '' AND
forme_juridique is not null and trim(forme_juridique) <> '' and numero_regis_commerce is not null and trim(numero_regis_commerce) <> '' AND
numero_identification_fiscale is not null and trim(numero_identification_fiscale) <> '' and ville_siege_social is not null and trim(ville_siege_social) <> '' AND
id_interne_mandataire is not null and trim(id_interne_mandataire) <>'' and qualite_mandataire is not null and trim(qualite_mandataire) <> '' AND
numero_piece_mandataire is not null and  trim(numero_piece_mandataire) <> '' and qualite_mandataire is not null and trim(qualite_mandataire) <> '' AND 
date_expiration_piece_mandataire is not null and trim(date_expiration_piece_mandataire) <> '' and lieu_emission_piece_mandataire is not null and trim(lieu_emission_piece_mandataire) <> '' AND
nom_mandataire is not null and trim(nom_mandataire) <> '' and prenom_mandataire is not null and trim(prenom_mandataire) <> ''
THEN 'OUI'
ELSE 'NON'
END ) as est_conforme_hors_cip,
(CASE WHEN iban is not null and trim(iban) <> '' AND numero_compte is not null and trim(numero_compte) <> '' AND raison_sociale is not null and trim(raison_sociale) <> '' AND
forme_juridique is not null and trim(forme_juridique) <> '' and numero_regis_commerce is not null and trim(numero_regis_commerce) <> '' AND
numero_identification_fiscale is not null and trim(numero_identification_fiscale) <> '' and ville_siege_social is not null and trim(ville_siege_social) <> '' AND
id_interne_mandataire is not null and trim(id_interne_mandataire) <>'' and qualite_mandataire is not null and trim(qualite_mandataire) <> '' AND
numero_piece_mandataire is not null and  trim(numero_piece_mandataire) <> '' and qualite_mandataire is not null and trim(qualite_mandataire) <> '' AND 
date_expiration_piece_mandataire is not null and trim(date_expiration_piece_mandataire) <> '' and lieu_emission_piece_mandataire is not null and trim(lieu_emission_piece_mandataire) <> '' AND
nom_mandataire is not null and trim(nom_mandataire) <> '' and prenom_mandataire is not null and trim(prenom_mandataire) <> ''
and trim(ibu) <> '' and ibu is not null and trim(date_creation) <> '' and date_creation is not null and trim(duree_de_vie) <> '' and duree_de_vie is not null and
trim(sigle_raison_sociale) <> '' and sigle_raison_sociale is not null and trim(secteur_activite_eco) <> '' and secteur_activite_eco is not null and trim(pays_siege_social) <> '' and 
pays_siege_social is not null and trim(nature_client_titulaire_compte) <> '' and nature_client_titulaire_compte is not null and trim(type_compte) <> '' and type_compte is not null and
trim(nature_compte) <> '' and nature_compte is not null and trim(code_devise) <> '' and code_devise is not null and trim(statut_compte) <> '' and statut_compte is not null and
trim(numero_ibu_mandataire) <> '' and numero_ibu_mandataire is not null and trim(situation_judiciaire) <> '' and situation_judiciaire is not null and trim(date_debut_interdiction_judiciaire) <> '' and
date_debut_interdiction_judiciaire is not null and trim(date_fin_interdiction_judiciaire) <> '' and date_fin_interdiction_judiciaire is not null and trim(nationalite_mandataire) <> '' and 
nationalite_mandataire is not null and trim(date_naissance_mandataire) <> '' and date_naissance_mandataire is not null and trim(numero_compte_om_mandataire) <> '' and numero_compte_om_mandataire is not null and
trim(id_complete_mandataire) <> '' and id_complete_mandataire is not null
THEN 'OUI'
ELSE 'NON'
END ) as est_conforme_cip,
est_multicompte_om,
est_actif_30,
est_actif_90,
est_client_telco,
est_suspendu_om,
est_suspendu_telco,
event_date
FROM (SELECT * FROM TMP.TT_BDI_OM_KYA_STEP_3 ) A
LEFT JOIN (SELECT * FROM  TMP.TT_BDI_OM_KYA_STEP_4) B
ON A.piece_identite = B.numeropiece