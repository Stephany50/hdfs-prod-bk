insert into tt.spark_cbm_data_2
select concat(bdle_name, '_voix'),date,subscription_channel,souscriptions/2,revenu*coef_voix/100,paiement,'Voix',coef_data,coef_voix,0,CA_voix
from tt.spark_cbm_data_1
where type = 'Combo'