select 
msisdn,
date_format(activation_date ,'dd.MM.yyyy hh:mm:ss') AS date_identification,
identificateur,
genre,
civilite,
est_snappe
from MON.SPARK_ACTIVATION_ALL_BY_DAY
WHERE EVENT_DATE  = "###SLICE_VALUE###"