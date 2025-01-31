insert into MON.SPARK_FT_ALIGNEMENT_TANGO_TELCO
select
msisdn,
nom_telco,
prenom_telco,
nom_prenom_telco,
date_naissance_telco,
numero_piece_telco,
est_suspendu,
odbincomingcalls,
odboutgoingcalls,
statut_bscs,
date_activation,
date_changement_statut,
date_expiration_telco,
adresse_telco,
statut_validation_bo_telco,
date_mise_a_jour_bo_telco,
prenom_om,
nom_om,
nom_prenom_om,
date_naissance_om,
numero_piece_om,
modified_on,
date_creation_om,
user_id,
addresse_om,
statut_bo_om,
date_mise_a_jour_bo_om,
date_expiration_om,
current_timestamp() as insert_date,
'###SLICE_VALUE###' as event_date
from TMP.KYC_TT_ALIGN4