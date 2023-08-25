insert into mon.spark_ft_qualif_imso PARTITION(event_date)
select 
A.msisdn,
agent_terrain,
loginbackoffice, 
date_emission,
etat, 
commentaire,
date_expiration_piece,
est_suspendu, 
cni_expire, 
date_expiration,
date_controle
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
              date_expiration_piece,
              date_controle,
              ROW_NUMBER() OVER( partition by msisdn ORDER BY ordre asc) row_num,
              '###SLICE_VALUE###' event_date
              from 
              (select msisdn, agent_terrain, loginbackoffice, date_emission, etat, commentaire, date_expiration_piece, date_controle, '1' ordre from cdr.spark_it_qualif_imso where event_date='###SLICE_VALUE###'
              union
              select msisdn, agent_terrain, loginbackoffice, date_emission, etat, commentaire, date_expiration_piece, date_controle, '2' ordre from mon.spark_ft_qualif_imso where event_date = date_sub('###SLICE_VALUE###', 1)
              ) T
       )E
       WHERE row_num <= 1
) A
left join
(
select msisdn, est_suspendu, cni_expire, date_expiration, event_date from MON.SPARK_FT_KYC_BDI_PP where event_date = '###SLICE_VALUE###'
) B
on A.msisdn = B.msisdn