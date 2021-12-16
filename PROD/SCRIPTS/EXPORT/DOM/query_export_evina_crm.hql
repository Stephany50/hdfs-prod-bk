select count(*) from (
select
distinct 
TicketNumber,
onc_Numeroappelant,
fileAttente,
Date_Interaction,
categorie,
typarticle,
article,
motif,
comment_interact
from cdr.spark_it_crm_reporting 
WHERE original_file_date  ='###SLICE_VALUE###' and onc_numeroappelant is not null and length(GET_NNP_MSISDN_9DIGITS(trim(onc_numeroappelant)))=9

