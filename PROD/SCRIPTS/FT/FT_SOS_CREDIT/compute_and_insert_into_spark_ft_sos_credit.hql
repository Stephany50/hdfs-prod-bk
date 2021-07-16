insert into MON.SPARK_FT_SOS_CREDIT
select 
     A.loan_id loan_id,
     A.loan_date loan_date,
     A.msisdn  msisdn,
     A.loan_service loan_service,
     A.loan_amount  loan_amount,
     A.service_fee  service_fee,
     (
          case 
               when upper(B.townname) ='DOUALA' then 'a.DOUALA'
               when upper(B.townname) ='YAOUNDE' then 'b.YAOUNDE'
               when upper(B.administrative_region) ='LITTORAL' then 'c.LITTORAL'
               when upper(B.administrative_region) ='CENTRE' then 'd.CENTRE'
               when upper(B.administrative_region) ='EXTREME-NORD' then 'e.EXTREME-NORD'
               when upper(B.administrative_region) ='NORD' then 'f.NORD'
               when upper(B.administrative_region) ='ADAMAOUA' then 'g.ADAMAOUA'
               when upper(B.administrative_region) ='NORD-OUEST' then 'h.NORD-OUEST'
               when upper(B.administrative_region) ='SUD-OUEST' then 'i.SUD-OUEST'
               when upper(B.administrative_region) ='OUEST' then 'j.OUEST'
               when upper(B.administrative_region) ='SUD' then 'k.SUD'
               when upper(B.administrative_region) ='EST' then 'l.EST'
          else 'N/A' end
     ) Region_Administrative,
     current_timestamp() insert_date,
     '###SLICE_VALUE###' event_date
from (
     select 
          A.loan_id,
          A.loan_date,
          A.msisdn,
          A.loan_service,
          A.loan_amount,
          A.service_fee
     from (
          SELECT DISTINCT
               PAYMENT_ID  LOAN_ID
               , PAY_DATE  LOAN_DATE
               , SUBSTR(ACC_NBR,4,9) MSISDN
               , 'CREDIT' LOAN_SERVICE
               , -(BILL_AMOUNT)/100 LOAN_AMOUNT
               ,(-(BILL_AMOUNT)/100)*0.1 SERVICE_FEE
          FROM CDR.SPARK_IT_ZTE_RECHARGE
          WHERE PAY_DATE = '###SLICE_VALUE###' and ACCT_RES_CODE = 20
     ) A 
     left join (
          SELECT * 
          FROM mon.spark_FT_CONTRACT_SNAPSHOT 
          WHERE EVENT_DATE = '###SLICE_VALUE###' AND OSP_STATUS <> 'TERMINATED'
     ) B 
     on A.msisdn = B.ACCESS_KEY
) A 
left join (
     select 
          msisdn,
          administrative_region,
          townname 
     from mon.spark_ft_client_last_site_day
     where event_date ='###SLICE_VALUE###'
) B
on A.msisdn = B.msisdn 
UNION
select 
     A.loan_id loan_id,
     A.loan_date loan_date,
     A.msisdn  msisdn,
     A.loan_service loan_service,
     A.loan_amount loan_amount,
     A.service_fee service_fee,
     (
          case 
               when upper(B.townname) ='DOUALA' then 'a.DOUALA'
               when upper(B.townname) ='YAOUNDE' then 'b.YAOUNDE'
               when upper(B.administrative_region) ='LITTORAL' then 'c.LITTORAL'
               when upper(B.administrative_region) ='CENTRE' then 'd.CENTRE'
               when upper(B.administrative_region) ='EXTREME-NORD' then 'e.EXTREME-NORD'
               when upper(B.administrative_region) ='NORD' then 'f.NORD'
               when upper(B.administrative_region) ='ADAMAOUA' then 'g.ADAMAOUA'
               when upper(B.administrative_region) ='NORD-OUEST' then 'h.NORD-OUEST'
               when upper(B.administrative_region) ='SUD-OUEST' then 'i.SUD-OUEST'
               when upper(B.administrative_region) ='OUEST' then 'j.OUEST'
               when upper(B.administrative_region) ='SUD' then 'k.SUD'
               when upper(B.administrative_region) ='EST' then 'l.EST'
          else 'N/A' end
     ) Region_Administrative,
     current_timestamp() insert_date,
     '###SLICE_VALUE###' event_date
from (
     select 
          NULL as loan_id,
          original_file_date loan_date,
          substr(msisdn,4,9)  msisdn,
          'VOICE' loan_service,
          nvl(amount,0)/100 loan_amount, 
          (nvl(amount,0)/100)*0.1 service_fee
     from cdr.spark_it_zte_loan_cdr
     where original_file_date ='###SLICE_VALUE###' and upper(transaction_type) ='LOAN'
) A left join
(
     select 
          msisdn,
          administrative_region,
          townname 
     from mon.spark_ft_client_last_site_day 
     where event_date ='###SLICE_VALUE###'
) B
on A.msisdn = B.msisdn
UNION 
select 
     A.loan_id loan_id,
     A.loan_date loan_date,
     A.msisdn msisdn,
     A.loan_service loan_service,
     A.loan_amount  loan_amount,
     A.service_fee  service_fee,
(
     case 
          when upper(B.townname) ='DOUALA' then 'a.DOUALA'
          when upper(B.townname) ='YAOUNDE' then 'b.YAOUNDE'
          when upper(B.administrative_region) ='LITTORAL' then 'c.LITTORAL'
          when upper(B.administrative_region) ='CENTRE' then 'd.CENTRE'
          when upper(B.administrative_region) ='EXTREME-NORD' then 'e.EXTREME-NORD'
          when upper(B.administrative_region) ='NORD' then 'f.NORD'
          when upper(B.administrative_region) ='ADAMAOUA' then 'g.ADAMAOUA'
          when upper(B.administrative_region) ='NORD-OUEST' then 'h.NORD-OUEST'
          when upper(B.administrative_region) ='SUD-OUEST' then 'i.SUD-OUEST'
          when upper(B.administrative_region) ='OUEST' then 'j.OUEST'
          when upper(B.administrative_region) ='SUD' then 'k.SUD'
          when upper(B.administrative_region) ='EST' then 'l.EST'
     else 'N/A' end
) Region_Administrative,  
current_timestamp() insert_date,
'###SLICE_VALUE###' event_date
from 
(
     select 
          NULL as loan_id,
          transaction_date loan_date, 
          substr(msisdn,4,9) msisdn,
          'DATA' loan_service,
          nvl(amount,0)/100 loan_amount,
          (nvl(amount,0)/100)*0.1 service_fee
     from cdr.spark_it_zte_emergency_data 
     where transaction_date ='###SLICE_VALUE###' and transaction_type ='LOAN'
) A left join
(
     select
          msisdn,
          administrative_region,
          townname 
     from mon.spark_ft_client_last_site_day 
     where event_date ='###SLICE_VALUE###'
) B
on A.msisdn = B.msisdn
