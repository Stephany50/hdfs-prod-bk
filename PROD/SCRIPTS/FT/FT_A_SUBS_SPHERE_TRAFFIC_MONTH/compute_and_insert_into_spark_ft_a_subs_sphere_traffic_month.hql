insert into AGG.SPARK_FT_A_SUBS_SPHERE_TRAFFIC_MONTH
select FN_NNP_SIMPLE_DESTINATION(other_party) AS OPERATEUR
,count(distinct case when duree_sortant > 0 then other_party else '1' end) NB_NUM_APPELLES
,count(distinct case when duree_entrant > 0 then other_party else '1' end) NB_NUM_APPELANTS
,event_month
from mon.spark_ft_sphere_traffic_month
where trim(event_month) = '###SLICE_VALUE###'
and FN_NNP_SIMPLE_DESTINATION(served_msisdn) = 'OCM'
group by event_month,FN_NNP_SIMPLE_DESTINATION(other_party)