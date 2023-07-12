insert into MON.SPARK_FT_A_CXD_RECOUVREMENT_ENC
select 
    to_date(invoice_date) invoice_date,
    type_paiement,
    type_client,
    customer_segment categ,
    payment_methon_name payment_method,
    count(*) nbr,
    sum(montant) total_enc,
    current_timestamp insert_date,
    date_saisie event_date
from CDR.SPARK_IT_RAPPORT_DAILY where date_saisie="###SLICE_VALUE###" and customer_segment not in ("Dealer","Partner","Key Accounts","O-Shop")
group by 
    to_date(invoice_date),
    type_paiement,
    type_client,
    customer_segment, 
    payment_methon_name,
    date_saisie