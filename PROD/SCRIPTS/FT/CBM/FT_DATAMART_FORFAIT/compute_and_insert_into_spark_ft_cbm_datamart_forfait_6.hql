insert into MON.SPARK_FT_CBM_DATAMART_FORFAIT
select 
distinct A.bdle_name,
B.id_bdle_name,
A.date,
A.subscription_channel,
A.souscriptions,
A.revenu,
A.Paiement,
A.type,
A.coef_data,
A.coef_voix,
A.CA_data,
A.CA_voix,
A.date
from tt.spark_cbm_data_2 as A 
left join DIM.spark_dt_cbm_bundles_2 as B on A.bdle_name=B.bdle_name
group by A.bdle_name,B.id_bdle_name,A.date,A.subscription_channel,A.souscriptions,A.revenu,A.Paiement,A.type,A.coef_data,A.coef_voix,A.CA_data,A.CA_voix