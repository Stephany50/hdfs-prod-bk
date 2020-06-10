select IF(ft_stm.nb_stm = 0 
                and ftmsct.nb_ftmsct > datediff(last_day(to_date('###SLICE_VALUE###' || '-01')), to_date('###SLICE_VALUE###' || '-01'))
          ,'OK'
          ,'NOK')
from (select count(*) as nb_stm from MON.FT_SPHERE_TRAFFIC_MONTH where event_month='###SLICE_VALUE###') ft_stm
,(select count(distinct TRANSACTION_DATE) as nb_ftmsct from MON.FT_MSC_TRANSACTION 
  WHERE TRANSACTION_DATE BETWEEN to_date('###SLICE_VALUE###' || '-01') AND last_day(to_date('###SLICE_VALUE###' || '-01')))  ftmsct