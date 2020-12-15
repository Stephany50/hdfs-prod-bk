select a.*, b.* from mon.spark_ft_marketing_b2b a
INNER join DIM.DT_MARKETING_B2B b
on upper(a.commercial_offer)= upper(b.offre)
where event_date ='###SLICE_VALUE###'
and UPPER(commercial_offer) like '%FLEX PLUS %K%'