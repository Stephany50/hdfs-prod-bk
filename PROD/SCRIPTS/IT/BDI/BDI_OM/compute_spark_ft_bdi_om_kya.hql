INSERT INTO MON.SPARK_FT_BDI_OM_KYA
SELECT 
piece_identite,
iban,
rib,
ibu,
msisdn,
date_creaction,
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
identification_mandataire,
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
rrc,
ppe,
risque_AML,
groupe,
pro,
qualite_mandataire,
responsabilite_compte,
date_expiration_piece_mandataire,
lieu_emission_piece_mandataire,
nom_mandataire,
prenom_mandataire,
nationalite_mandataire,
date_naissance_mandataire,
(CASE WHEN iban is not null and trim(iban) <> '' AND msisdn is not null and trim(msisdn) <> '' AND raison_sociale is not null and trim(raison_sociale) <> '' AND
forme_juridique is not null and trim(forme_juridique) <> '' and numero_regis_commerce is not null and trim(numero_regis_commerce) <> '' AND
numero_identification_fiscale is not null and trim(numero_identification_fiscale) <> '' and ville_siege_social is not null and trim(ville_siege_social) <> '' AND
identification_mandataire is not null and trim(identification_mandataire) <>'' and qualite_mandataire is not null and trim(qualite_mandataire) <> '' AND
numero_piece_mandataire is not null and  trim(numero_piece_mandataire) <> '' and qualite_mandataire is not null and trim(qualite_mandataire) <> '' AND 
date_expiration_piece_mandataire is not null and trim(date_expiration_piece_mandataire) <> '' and lieu_emission_piece_mandataire is not null and trim(lieu_emission_piece_mandataire) <> '' AND
nom_mandataire is not null and trim(nom_mandataire) <> '' and prenom_mandataire is not null and trim(prenom_mandataire) <> ''
THEN 'OUI'
ELSE 'NON'
END ) as est_conforme_beac,
est_multicompte_om,
est_actif_30,
est_actif_90,
est_client_telco,
est_suspendu_om,
est_suspendu_telco,
event_date
FROM (SELECT * FROM TMP.TT_BDI_OM_KYA_1 ) A
LEFT JOIN (SELECT * FROM  TMP.TT_BDI_OM_KYA_2) B
ON A.piece_identite = B.numeropiece