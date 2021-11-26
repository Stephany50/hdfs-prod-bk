select
TicketNumber,
onc_Numeroappelant,
fileAttente,
Date_Interaction,
categorie,
typarticle,
article,
motif,
comment_interact,
ORIGINAL_FILE_LINE_COUNT,
INSERT_DATE
from cdr.spark_it_crm_reporting 
WHERE original_file_date  ='###SLICE_VALUE###'