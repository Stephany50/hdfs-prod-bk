select 
nom,
prenom,
msisdn,
date_naissance,
type_personne,
statut,
nvl(DATEDIFF(date_expiration,current_date), 0) as expire_delay,
original_file_date
from CDR.SPARK_IT_KYC_BDI_FULL
where original_file_date = '###SLICE_VALUE###'