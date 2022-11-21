select 
logtimeDepot, 
msisdn ,
statut,
message ,
montantDepot ,
idtransaction ,
souscriptionTime ,
service,
etat
from CDR.SPARK_IT_END_OF_YEAR_WINNERS
WHERE ORIGINAL_FILE_DATE = '###SLICE_VALUE###'
  
