insert into mon.spark_ft_sos_voice
select A.msisdn msisdn ,
       A.price_plan_name  price_plan_code,
       A.montant  montant,
      (nvl(A.a_rembourser,0) + nvl(B.a_rembourser,0) +nvl(B.Payback_amount,0)) as A_REMBOURSER,
      (case when nvl(A.a_rembourser,0) + nvl(B.a_rembourser,0) +nvl(B.Payback_amount,0) = 0 then (B.LOAN_Amount/B.montant_unitaire +1)*A.montant*0.1
       else (nvl(B.LOAN_Amount,0)/nvl(B.montant_unitaire,0))*A.montant*0.1 end) as commission,
     (case when nvl(A.a_rembourser,0) + nvl(B.a_rembourser,0) +nvl(B.Payback_amount,0) = 0 then 'Terminated'
      else 'Encours' end) as status,
      B.original_file_date
from 
(select      msisdn,
    price_plan_name,
           montant,
       nvl(A_rembourser,0) A_REMBOURSER,
       commission,
          status,
   transaction_date
from mon.spark_ft_sos_voice
where transaction_date =date_sub('###SLICE_VALUE###',1) and A_rembourser != 0 and status='Encours') A inner join
(select A.original_file_date original_file_date,
        A.msisdn   msisdn,
        B.price_plan_name  price_plan_name,
        A.Loan_Amount Loan_Amount,
        A.montant_unitaire montant_unitaire,
        A.A_rembourser  A_rembourser,
        A.PAYBACK_Amount   Payback_Amount
from
(select     B.original_file_date   original_file_date,
           B.msisdn                      msisdn, 
           B.price_plan_code         price_plan_code,
           A.Total_montant_loan       LOAN_Amount,
           B.montant_unitaire        montant_unitaire,
           A.A_rembourser            A_rembourser,
           B.Total_montant_payback   Payback_Amount 
from
(select      original_file_date,
substring(msisdn,4,9)   msisdn,
               price_plan_code,
(case when price_plan_code ='1104010' then 500 
      when price_plan_code ='1104011' then 250
      else 100 end)           montant_unitaire,
     sum(nvl(amount,0))/100  Total_montant_payback
   from CDR.SPARK_IT_ZTE_LOAN_CDR 
where original_file_date ='###SLICE_VALUE###' and transaction_type='PAYBACK'
group by original_file_date, msisdn , price_plan_code) B left join
(select      original_file_date,
 substring(msisdn,4,9) msisdn,
              price_plan_code,
     sum(nvl(amount,0))/100  Total_montant_loan,
     sum(nvl(amount,0))*1.1/100 A_rembourser,
        0 as commission, 
        'Encours' as status
from CDR.SPARK_IT_ZTE_LOAN_CDR 
where original_file_date ='###SLICE_VALUE###' and transaction_type='LOAN'
group by original_file_date, msisdn , price_plan_code) A
on A.msisdn = B.msisdn and A.price_plan_code = B.price_plan_code) A left join
(select price_plan_name, price_plan_code
from cdr.spark_it_zte_price_plan_extract 
where original_file_date ='###SLICE_VALUE###' 
group by price_plan_name, price_plan_code) B
on A.price_plan_code = B.price_plan_code) B
on A.msisdn = B.msisdn  and A.price_plan_name = B.price_plan_name

