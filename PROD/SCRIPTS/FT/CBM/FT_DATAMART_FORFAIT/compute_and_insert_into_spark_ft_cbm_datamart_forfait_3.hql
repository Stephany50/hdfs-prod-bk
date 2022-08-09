insert into tt.spark_cbm_data_2
select concat(bdle_name, '_data'),date,subscription_channel,souscriptions/2,revenu*coef_data/100,paiement,'Data',coef_data,coef_voix,CA_data,0
from tt.spark_cbm_data_1
where type = 'Combo'