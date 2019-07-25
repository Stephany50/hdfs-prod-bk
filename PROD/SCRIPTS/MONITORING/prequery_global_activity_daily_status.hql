
SELECT IF( T.count_zero >= 13, 'NOK', 'OK')
FROM
(
select count(*)  count_zero from MON.GLOBAL_ACTIVITY_DAILY_STATUS where TRANSACTION_DATE = '###SLICE_VALUE###'
) T
