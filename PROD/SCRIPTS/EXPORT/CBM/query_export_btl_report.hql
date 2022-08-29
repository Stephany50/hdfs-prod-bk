SELECT msisdn,
type_forfait,
msisdn_vendeur,
prix,
ipp,
ipp_name,
ipp_stream,
transaction_date
FROM
mon.spark_ft_btl_report WHERE TO_DATE(transaction_date) ='###SLICE_VALUE###'