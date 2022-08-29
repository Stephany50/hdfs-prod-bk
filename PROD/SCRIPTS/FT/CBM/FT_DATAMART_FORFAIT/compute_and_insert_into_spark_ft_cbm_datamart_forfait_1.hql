create table tt.spark_cbm_data_1 as
select 
distinct A.bdle_name,
A.date,
A.subscription_channel,
A.souscriptions,
A.revenu,
A.Paiement,
B.type,
B.coef_data,
B.coef_voix,
A.revenu*B.coef_data/100 as CA_data,
A.revenu*B.coef_voix/100 as CA_voix
from tt.spark_cbm_data as A 
left join DIM.spark_dt_cbm_bundles_1 as B on A.bdle_name=B.bdle_name
group by A.bdle_name,A.date,A.subscription_channel,A.souscriptions,A.revenu,A.Paiement,B.type,B.coef_data,B.coef_voix,A.revenu*B.coef_data/100,A.revenu*B.coef_voix/100