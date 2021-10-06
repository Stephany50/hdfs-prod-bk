insert into TMP.tt_flotte3
select * from (
select a.*,
row_number() over(partition by msisdn order by date_souscription desc nulls last) as rang
from TMP.tt_flotte2 a) where rang = 1
union all
select * from (
select a.*,
row_number() over(partition by msisdn order by date_souscription desc nulls last) as rang
from TMP.tt_flotte1 a) where rang = 1