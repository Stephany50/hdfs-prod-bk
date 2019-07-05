add jar hdfs:///PROD/UDF/hive-udf-1.0.jar;
create temporary function FN_FORMAT_MSISDN_TO_9DIGITS as 'cm.orange.bigdata.udf.FormatMsisdnTo9Digits';
create temporary function fn_get_nnp_msisdn as 'cm.orange.bigdata.udf.GetNnpMsisdn';


--insert into FT_LAST_UPDATE_EC_EXTRACT
SELECT
  NVL(JOUR_J.msisdn,PREV.msisdn) msisdn
     ,NVL(JOUR_J.loan_amount,PREV.loan_amount) loan_amount
     ,NVL(JOUR_J.loan_to_pay,PREV.loan_to_pay) loan_to_pay
     ,NVL(JOUR_J.STATUS,PREV.STATUS) STATUS
     ,NVL(JOUR_J.ec_date,PREV.ec_date) ec_date
     ,NVL(JOUR_J.last_payment_date,PREV.last_payment_date) last_payment_date
     ,NVL(JOUR_J.fee_amount,PREV.fee_amount) fee_amount
     ,'IT_ZTE_EMERGENCY_CREDIT' Original_file_name
     ,DATE_FORMAT('20190102', 'YYYYMMDD') Original_file_date
FROM (
       select
         distinct
         msisdn
                , first_value(loan_amount)over(partition by msisdn order by ec_date desc) loan_amount
                , sum(loan_amount)over(partition by msisdn) + sum(loan_pay)over(partition by msisdn) loan_to_pay
                , case
                    when  sum(loan_amount)over(partition by msisdn) + sum(loan_pay)over(partition by msisdn) >0 then 'A'
         end STATUS
                , max (ec_date) over(partition by msisdn) ec_date
                , max(last_payment_date)over(partition by msisdn)  last_payment_date
                , 0 fee_amount
       from
         (
           select
             FN_FORMAT_MSISDN_TO_9DIGITS(fn_get_nnp_msisdn (msisdn)) msisdn
                , case
                    when transaction_type = 'LOAN' then amount/100
                    else 0
             end Loan_amount
                , case
                    when transaction_type = 'PAYBACK' then amount/100
                    else 0
             end Loan_pay
                , 0 Loan_to_pay
                , 'A' Status
                , case
                    when transaction_type = 'LOAN' then transaction_date
                    else date_format('19990101','YYYYMMDD')
             end  ec_date
                , case
                    when transaction_type = 'PAYBACK' then transaction_date
                    else date_format('19990101','YYYYMMDD')
             end Last_payment_date
           from CDR.IT_ZTE_EMERGENCY_CREDIT
           where transaction_date ='2019-06-29'
           --and msisdn in ('237698769733', '237697330139', '237695018827', '237658422305')
         )T1
     )JOUR_J
       LEFT OUTER JOIN (
  SELECT
    msisdn
       ,loan_amount
       , case
           when loan_to_pay < 0 then 0
           else loan_to_pay
    end loan_to_pay
       , status
       , ec_date
       , last_payment_date
       ,fee_amount
       , 'IT_ZTE_EMERGENCY_CREDIT' Source_file
       ,DATE_FORMAT('2019-06-29', 'YYYYMMDD') event_date
  from MON.FT_LAST_UPDATE_EC_EXTRACT
  where event_date = DATE_SUB('2019-06-29',1)
)PREV ON JOUR_J.msisdn=PREV.msisdn
WHERE JOUR_J.msisdn IS NULL OR PREV.msisdn IS NULL
UNION
SELECT
  msisdn
     ,pret_en_cours Loan_amount
     , case
         when remb_H < 0 then Remb_J
         else Remb_H + Remb_J
  end Loan_to_pay
     , 'A' Status
     , case
         when  nvl(PRET_J_DATE,  DATE_FORMAT('19990101', 'YYYYMMDD')) >  PRET_H_DATE then PRET_J_DATE
         else PRET_H_DATE
  end  ec_date
     , case
         when  nvl(REMB_J_DATE,  DATE_FORMAT('19990101', 'YYYYMMDD')) >  REMB_H_DATE then REMB_J_DATE
         else REMB_H_DATE
  end last_payment_date
     ,fee fee_amount
     , 'IT_ZTE_EMERGENCY_CREDIT' Source_file
     ,DATE_FORMAT('2019-06-29', 'YYYYMMDD') event_date
FROM (
       SELECT
         JOUR_J.msisdn msisdn
            ,JOUR_J.loan_amount pret_J
            ,JOUR_J.loan_to_pay Remb_J
            ,JOUR_J.EC_DATE pret_J_date
            ,JOUR_J.last_payment_date Remb_J_date
            ,PREV.loan_amount pret_H
            ,PREV.loan_to_pay Remb_H
            ,PREV.EC_DATE pret_H_date
            ,PREV.last_payment_date Remb_H_date
            ,case
               when nvl(JOUR_J.loan_amount, 0) = 0 then PREV.loan_amount
               else JOUR_J.loan_amount
         end pret_en_cours
            ,PREV.ec_date ec_date
       FROM (
              select
                distinct
                msisdn
                       , first_value(loan_amount)over(partition by msisdn order by ec_date desc) loan_amount
                       , sum(loan_amount)over(partition by msisdn) + sum(loan_pay)over(partition by msisdn) loan_to_pay
                       , case
                           when  sum(loan_amount)over(partition by msisdn) + sum(loan_pay)over(partition by msisdn) >0 then 'A'
                end STATUS
                       , max (ec_date) over(partition by msisdn) ec_date
                       , max(last_payment_date)over(partition by msisdn)  last_payment_date
                       , 0 fee_amount
              from
                (
                  select
                    FN_FORMAT_MSISDN_TO_9DIGITS(fn_get_nnp_msisdn (msisdn)) msisdn
                       , case
                           when transaction_type = 'LOAN' then amount/100
                           else 0
                    end Loan_amount
                       , case
                           when transaction_type = 'PAYBACK' then amount/100
                           else 0
                    end Loan_pay
                       , 0 Loan_to_pay
                       , 'A' Status
                       , case
                           when transaction_type = 'LOAN' then transaction_date
                           else date_format('19990101','YYYYMMDD')
                    end  ec_date
                       , case
                           when transaction_type = 'PAYBACK' then transaction_date
                           else date_format('19990101','YYYYMMDD')
                    end Last_payment_date
                  from CDR.IT_ZTE_EMERGENCY_CREDIT
                  where transaction_date ='2019-06-29'
                  --and msisdn in ('237698769733', '237697330139', '237695018827', '237658422305')
                )T1
            )JOUR_J
              INNER JOIN (
         SELECT
           msisdn
              ,loan_amount
              , case
                  when loan_to_pay < 0 then 0
                  else loan_to_pay
           end loan_to_pay
              , status
              , ec_date
              , last_payment_date
              ,fee_amount
         from MON.FT_LAST_UPDATE_EC_EXTRACT
         where event_date = DATE_SUB('2019-06-29',1)
       )PREV ON JOUR_J.msisdn=PREV.msisdn

     )a,
     dim.SPLIT_FEE_EMERGENCY_CREDIT b
where nvl(a.pret_en_cours, 0) = b.loan_amount
  and datediff('2019-06-29',to_date(ec_date) )> Marge_Min
  and datediff('2019-06-29',to_date(ec_date) )<= Marge_Max  ;;