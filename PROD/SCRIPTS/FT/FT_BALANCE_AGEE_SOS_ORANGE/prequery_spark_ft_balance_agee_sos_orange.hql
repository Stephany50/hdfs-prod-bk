select 
  IF(
          ft_sos_reports.nb_sos_reports  > 0 AND
          ft_balance_agee.nb_balance_agee = 0 AND
          ,'OK'
          ,'NOK')
from (
  select count(*) nb_sos_reports
  from MON.SPARK_FT_SOS_ORANGE_REPORTS
  where event_date ='###SLICE_VALUE###'
) ft_sos_reports,
( 
    select count(*) as nb_balance_agee
    from MON.SPARK_FT_BALANCE_AGEE_SOS_ORANGE
    where EVENT_DATE = '###SLICE_VALUE###'
) ft_balance_agee


