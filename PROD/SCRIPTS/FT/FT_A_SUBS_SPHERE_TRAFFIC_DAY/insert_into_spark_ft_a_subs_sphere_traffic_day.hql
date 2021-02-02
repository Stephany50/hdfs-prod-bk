insert into AGG.SPARK_FT_A_SUBS_SPHERE_TRAFFIC_DAY
select
operateur,
nb_num_appelles,
nb_num_appelants,
current_timestamp() as insert_date,
event_date
from TMP.tt_uniq_subs_jour_ind