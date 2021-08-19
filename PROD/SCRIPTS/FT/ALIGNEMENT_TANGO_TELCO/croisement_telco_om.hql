insert into TMP.KYC_TT_ALIGN3
select
a.msisdn,
a.nom_telco,
a.prenom_telco,
a.nom_prenom_telco,
a.date_naissance_telco,
a.numero_piece_telco,
a.est_suspendu,
a.odbincomingcalls,
a.odboutgoingcalls,
a.statut_bscs,
a.date_activation,
a.date_changement_statut,
a.date_expiration as date_expiration_telco,
a.adresse as adresse_telco,
a.statut_validation_bo_telco,
a.date_mise_a_jour_bo_telco,
b.prenom_om,
b.nom_om,
b.nom_prenom_om,
b.date_naissance_om,
b.numero_piece_om,
b.modified_on,
b.date_creation_om,
b.user_id,
b.addresse_om,
b.statut_bo_om,
b.date_mise_a_jour_bo_om,
b.date_expiration_om
from
(select *
from TMP.KYC_TT_ALIGN1) a
join (select *
from TMP.KYC_TT_ALIGN2) b on trim(a.msisdn) = trim(b.msisdn)