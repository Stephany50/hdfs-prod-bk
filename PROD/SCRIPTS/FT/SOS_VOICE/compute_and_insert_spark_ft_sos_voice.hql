insert into mon.spark_ft_sos_voice
select A.msisdn,
       A.price_plan_name,
       A.Loan_Amount  montant,
       A.A_REMBOURSER a_rembourser,
       0 as commission,
      'Encours'as status,
       A.transaction_date transaction_date
from
(select   A.transaction_date transaction_date,
         A.msisdn   msisdn,
         B.price_plan_name  price_plan_name,
        A.Loan_Amount Loan_Amount,
        A.A_rembourser  A_rembourser,
        A.PAYBACK_Amount   Payback_Amount
from 
(select     A.transaction_date   transaction_date, 
           A.msisdn                      msisdn, 
           A.price_plan_code         price_plan_code,
           A.montant                LOAN_Amount,
           A.A_rembourser     A_rembourser,
           nvl(B.montant,0)          Payback_Amount 
from
(select      transaction_date,
 substring(msisdn,4,9) msisdn,
              price_plan_code,
     sum(nvl(amount,0))/100  montant,
     sum(nvl(amount,0))*1.1/100 A_rembourser,
        0 as commission, 
        'Encours' as status
from CDR.SPARK_IT_ZTE_LOAN_CDR 
where transaction_date ='###SLICE_VALUE###' and transaction_type='LOAN'
group by transaction_date, msisdn , price_plan_code) A left join
(select      transaction_date,   
substring(msisdn,4,9)   msisdn,
               price_plan_code,
     sum(nvl(amount,0))/100  montant
   from CDR.SPARK_IT_ZTE_LOAN_CDR 
where transaction_date ='###SLICE_VALUE###' and transaction_type='PAYBACK'
group by transaction_date, msisdn , price_plan_code) B
on A.msisdn = B.msisdn and A.price_plan_code = B.price_plan_code
where B.msisdn is null and B.price_plan_code is null) A left join
(select price_plan_name, price_plan_code
from cdr.spark_it_zte_price_plan_extract 
where original_file_date ='###SLICE_VALUE###' 
group by price_plan_name, price_plan_code) B
on A.price_plan_code = B.price_plan_code) A

