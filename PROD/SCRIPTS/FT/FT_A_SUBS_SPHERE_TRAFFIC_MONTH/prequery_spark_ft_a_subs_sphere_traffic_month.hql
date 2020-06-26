select IF(ft_stm.nb_stm >= 10 )
          ,'OK'
          ,'NOK')
from (select count(*) as nb_stm from MON.SPARK_FT_SPHERE_TRAFFIC_MONTH where event_month='###SLICE_VALUE###') ft_stm