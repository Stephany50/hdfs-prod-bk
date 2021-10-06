insert into TMP.tt_uniq_subs_jour_ind
select FN_NNP_SIMPLE_DESTINATION(other_party) AS OPERATEUR
,count(distinct case when duree_sortant > 0 then other_party else '1' end) NB_NUM_APPELLES
,count(distinct case when duree_entrant > 0 then other_party else '1' end) NB_NUM_APPELANTS
,Transaction_date as event_date
from TMP.tt_uniq_subs_jour
where transaction_date = '###SLICE_VALUE###'
and FN_NNP_SIMPLE_DESTINATION(served_msisdn) = 'OCM'
group by Transaction_date,FN_NNP_SIMPLE_DESTINATION(other_party)