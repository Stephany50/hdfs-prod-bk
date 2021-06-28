insert into mon.spark_ft_sos_voice
select A.msisdn msisdn ,
       A.price_plan_name price_plan_name,
       A.montant montant,
       A.a_rembourser a_rembourser,
       A.commission commission,
       A.status status,
       date_add(A.transaction_date,1) transaction_date
from 
(select      msisdn,
    price_plan_name,
           montant,
       A_rembourser,
       commission,
          status,
   transaction_date
from mon.spark_ft_sos_voice
where transaction_date =date_sub('###SLICE_VALUE###',1) and A_rembourser != 0 and status='Encours') A left join
(select A.original_file_date original_file_date,
        A.msisdn msisdn,
    B.price_plan_name price_plan_name,
    A.montant montant
from
(select  original_file_date,
     substring(msisdn,4,9)   msisdn,
                    price_plan_code,
     sum(nvl(amount,0))/100  montant
from CDR.SPARK_IT_ZTE_LOAN_CDR 
where original_file_date ='###SLICE_VALUE###' and transaction_type='PAYBACK'
group by original_file_date, msisdn , price_plan_code) A left join
(select price_plan_name, price_plan_code
from cdr.spark_it_zte_price_plan_extract 
where original_file_date ='###SLICE_VALUE###' 
group by price_plan_name, price_plan_code) B
on A.price_plan_code = B.price_plan_code ) B
on A.msisdn = B.msisdn  and A.price_plan_name = B.price_plan_name
where B.msisdn is null

