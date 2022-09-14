insert into tt.subscription_om_dynamique
select 
    msisdn,
    bdle_name,
    bdle_cost 
from cdr.dt_Services_dynamique