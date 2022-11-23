select 
logtime ,
montant,  
msisdn ,
service ,
idtransaction,
montantDepot
from CDR.SPARK_IT_END_OF_YEAR
WHERE ORIGINAL_FILE_DATE = '###SLICE_VALUE###'
  
