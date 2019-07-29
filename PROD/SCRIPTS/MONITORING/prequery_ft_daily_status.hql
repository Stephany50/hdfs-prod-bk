
SELECT IF((T.count_total = 0 or T.count_zero > 0) or T.count_total<40 , 'OK', 'NOK')
FROM
(
select count(*) count_total, nvl(sum(case when nb_rows = 0 then 1 else 0 end), 0) count_zero from FT_DAILY_STATUS where table_date = '###SLICE_VALUE###'
) T
