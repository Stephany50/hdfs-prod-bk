select  ACTIVATION_DATE,
IDENTIFICATEUR,
MSISDN_COUNT
from MON.SPARK_FT_ACTIVATION_BY_IDENTIF_DAY
where ACTIVATION_DATE= "###SLICE_VALUE###"