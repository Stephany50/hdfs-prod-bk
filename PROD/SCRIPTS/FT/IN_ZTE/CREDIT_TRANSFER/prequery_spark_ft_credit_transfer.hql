SELECT IF(COUNT(*) = 0, 'OK','NOK') 
FROM MON.FT_CREDIT_TRANSFER
WHERE REFILL_DATE = '###SLICE_VALUE###'
