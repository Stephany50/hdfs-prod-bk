

insert into AGG.SPARK_FT_A_DATAMART_OM_MARKETING
SELECT
region ADMINISTRATIVE_REGION,
PROFILE PROFILE_CODE,
DETAILS,
sum(VAL) val,
sum(VOL) vol,
sum(REVENU) revenu,
sum(COMMISSION) COMMISSION,
sum(1) NB_LIGNES,
count(distinct msisdn) NB_NUMEROS,
STYLE,
SERVICE_TYPE,
OPERATOR_CODE,
current_timestamp INSERT_DATE,
to_date(JOUR) jour
from (select * from MON.SPARK_DATAMART_OM_MARKETING2 where jour='###SLICE_VALUE###')  a
left join  (
    select * from mon.spark_ft_contract_snapshot
    where event_date>='###SLICE_VALUE###'
) b on to_date(a.jour)=b.event_date and a.msisdn=b.access_key
left join (select ci,max(region) region from dim.dt_gsm_cell_code group by ci) c on cast(c.ci as int)=cast(b.location_ci as int)

group by region,PROFILE,DETAILS,STYLE,SERVICE_TYPE,
OPERATOR_CODE,to_date(JOUR)
