select 
  IF(
          ft_sosc.nb_sosc = 0 AND
          ft_cs.nb_cs > 0 AND
          ft_clsd.nb_ftclsd > 0
          ,'OK'
          ,'NOK')
from 
(
  select count(*) nb_sosc
  from MON.SPARK_FT_SOS_CREDIT
  where event_date ='###SLICE_VALUE###'
) ft_sosc,
( 
    select count(*) as nb_cs 
    from mon.spark_FT_CONTRACT_SNAPSHOT 
    where EVENT_DATE = '###SLICE_VALUE###'
) ft_cs
,
(
    select count(*) as nb_ftclsd
    from mon.spark_ft_client_last_site_day
    WHERE event_date ='###SLICE_VALUE###' 
) ft_clsd

