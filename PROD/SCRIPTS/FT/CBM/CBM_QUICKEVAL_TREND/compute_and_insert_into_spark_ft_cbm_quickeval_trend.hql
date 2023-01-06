insert into mon.spark_ft_cbm_quickeval_trend
select _date,
    temoin,
    ipp1,
    problematique,
    ca_forfait_periode2,
    volume_data_periode2,
    mou_periode2
from 
    mon.spark_ft_cbm_quick_eval
