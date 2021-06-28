insert into mon.spark_ft_sos_voice
select A.msisdn msisdn,
       A.price_plan_name,
       A.montant_unitaire  montant,
      (nvl(A.a_rembourser,0) + nvl(A.Payback_amount,0))  A_rembourser,
      (case when  nvl(A.a_rembourser,0) + nvl(A.Payback_amount,0) = 0 then (A.LOAN_Amount/A.montant_unitaire)*0.1*montant_unitaire
      else (A.LOAN_Amount/A.montant_unitaire -1)*0.1*montant_unitaire end) as commission,
     (case when nvl(A.a_rembourser,0) + nvl(A.Payback_amount,0) = 0 then 'Terminated'
     else 'Encours' end) as status,
      A.transaction_date
from
(select   A.transaction_date transaction_date,
         A.msisdn   msisdn,
         B.price_plan_name  price_plan_name,
        A.montant_unitaire  montant_unitaire,
        A.Loan_Amount Loan_Amount,
        A.A_rembourser  A_rembourser,
        A.PAYBACK_Amount   Payback_Amount
from
(select     B.transaction_date   transaction_date,
           B.msisdn                      msisdn,
           B.price_plan_code         price_plan_code,
           A.montant_unitaire        montant_unitaire,
           A.Montant_Total_loan         LOAN_Amount,
           A.A_rembourser     A_rembourser,
           B.Montant_total_payback    Payback_Amount
from
(select      transaction_date,
substring(msisdn,4,9)   msisdn,
               price_plan_code,
     sum(nvl(amount,0))/100  Montant_total_payback
   from CDR.SPARK_IT_ZTE_LOAN_CDR
where transaction_date ='###SLICE_VALUE###' and transaction_type='PAYBACK'
group by transaction_date, msisdn , price_plan_code) B inner join
(select            transaction_date,
       substring(msisdn,4,9) msisdn,
                    price_plan_code,
(case when price_plan_code ='1104010' then 500
when price_plan_code ='1104011' then 250
else  100 end)  montant_unitaire,
     sum(nvl(amount,0))/100  Montant_Total_loan,
    sum(nvl(amount,0))*1.1/100 A_rembourser,
        0 as commission,
        'Encours' as status
from CDR.SPARK_IT_ZTE_LOAN_CDR
where transaction_date ='###SLICE_VALUE###' and transaction_type='LOAN'
group by transaction_date, msisdn , price_plan_code) A
on A.msisdn = B.msisdn and A.price_plan_code = B.price_plan_code) A left join
(select price_plan_name, price_plan_code
from cdr.spark_it_zte_price_plan_extract
where original_file_date ='###SLICE_VALUE###'
group by price_plan_name, price_plan_code) B
on A.price_plan_code = B.price_plan_code) A left join
(select      msisdn,
    price_plan_name,
           montant,
       A_rembourser,
       commission,
          status,
   transaction_date
from mon.spark_ft_sos_voice
where transaction_date =date_sub('###SLICE_VALUE###',1) and A_rembourser != 0 and status='Encours') B
on A.msisdn = B.msisdn  and A.price_plan_name = B.price_plan_name
where B.msisdn is null and B.price_plan_name is null

