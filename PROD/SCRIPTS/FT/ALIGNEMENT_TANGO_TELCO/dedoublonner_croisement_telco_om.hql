insert into TMP.tt_align4
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
date_expiration_om
from (
select a.*,
row_number() over(partition by msisdn order by modified_on desc nulls last) as rn
from TMP.tt_align3 a
) b where rn = 1