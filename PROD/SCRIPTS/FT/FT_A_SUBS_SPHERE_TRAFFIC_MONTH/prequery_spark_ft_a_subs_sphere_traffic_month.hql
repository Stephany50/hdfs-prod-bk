select IF(ft_stm.nb_stm >= 10 and ft_aggstm.nb_aggstm  = 0
          ,'OK'
          ,'NOK')
from (select count(*) as nb_stm from MON.SPARK_FT_SPHERE_TRAFFIC_MONTH where event_month='###SLICE_VALUE###') ft_stm
,(select count(*) as nb_aggstm from AGG.SPARK_FT_A_SUBS_SPHERE_TRAFFIC_MONTH where event_month='###SLICE_VALUE###') ft_aggstm