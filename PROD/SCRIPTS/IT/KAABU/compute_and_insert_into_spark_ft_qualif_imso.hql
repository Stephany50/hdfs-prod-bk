insert into mon.spark_ft_qualif_imso PARTITION(event_date)
select 
A.msisdn,
agent_terrain,
loginbackoffice, 
date_emission,
etat, 
commentaire,
date_expiration,
periode_expiration, 
status, 
A.event_date
from 
(
       select * 
       from (
              select 
              msisdn, 
              agent_terrain, 
              loginbackoffice, 
              date_emission,
              etat,
              commentaire,
              ROW_NUMBER() OVER( partition by msisdn ORDER BY ordre) row_num,
              '###SLICE_VALUE###' event_date
              from 
              (select msisdn, agent_terrain, loginbackoffice, date_emission, etat, commentaire, '1' ordre from cdr.spark_it_qualif_imso where event_date='###SLICE_VALUE###'
              union
              select msisdn, agent_terrain, loginbackoffice, date_emission, etat, commentaire, '2' ordre from mon.spark_ft_qualif_imso where event_date = date_sub('###SLICE_VALUE###', 1)
              ) T
       )E
       WHERE row_num <= 1
) A
left join
(
select msisdn, date_expiration, periode_expiration, status, event_date from MON.SPARK_FT_CNI_EXPIREES where event_date = '###SLICE_VALUE###'
) B
on A.msisdn = B.msisdn