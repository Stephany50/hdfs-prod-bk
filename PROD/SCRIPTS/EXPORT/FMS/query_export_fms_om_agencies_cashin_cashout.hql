select
    shop,
    `date`,
    type,
    amount,
    channel,
    staff_name,
    transaction_id
from MON.SPARK_FT_FMS_OM_AGENCIES_CASHIN_CASHOUT
where transaction_date = '###SLICE_VALUE###'

