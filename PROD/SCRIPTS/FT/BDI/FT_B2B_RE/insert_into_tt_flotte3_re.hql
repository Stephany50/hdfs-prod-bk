insert into TMP.tt_flotte3_RE
select * from (
select a.*,
row_number() over(partition by msisdn order by date_souscription desc nulls last) as rang
from TMP.tt_flotte2_RE a) where rang = 1
union all
select * from (
select a.*,
row_number() over(partition by msisdn order by date_souscription desc nulls last) as rang
from TMP.tt_flotte1_RE a) where rang = 1