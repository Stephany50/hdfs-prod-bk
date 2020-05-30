INSERT INTO MON.SPARK_FT_STKAPRIM_REFILL_TYPE
select
a.stkaprim  stkaprim,
IF(a.stkaprim is null or b.msisdn is null,a.partenaire,b.partner_name) partenaire ,
a.type_event type_event,
a.event_simplifie event_simplifie,
a.amount amount,
a.commission commission,
a.capilarite  capilarite,
a.capat_cumul capat_cumul,
a.source source,
CURRENT_TIMESTAMP insert_date ,
a.refill_date refill_date
from
TMP.SPARK_FT_STKAPRIM_REFILL_TYPE a
full outer join
(
select a.msisdn, a.activ_begin_date, a.activ_end_date, b.grdparent, b.partner_name, b.activ_begin_date as parent_begin_date, b.activ_end_date as parent_end_date
from (select * from dim.spark_dim_stk_hierachy) a
INNER JOIN (select * from dim.spark_dim_stk_hierachy) b
ON (a.grdparent = b.msisdn and a.activ_begin_date >= b.activ_begin_date and a.activ_begin_date < nvl(b.activ_end_date, CURRENT_DATE))
) b
on (a.stkaprim = b.msisdn and refill_date = '###SLICE_VALUE###' and  refill_date >= activ_begin_date and refill_date < nvl(activ_end_date, CURRENT_DATE))