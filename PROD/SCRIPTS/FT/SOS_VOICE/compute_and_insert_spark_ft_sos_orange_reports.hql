insert into mon.spark_ft_sos_orange_reports
select A.MSISDN,A.SOS_TYPE,A.TRANSACTION_TYPE,A.MONTANT,A.COMMISSION,A.PRICE_PLAN_NAME ,A.CONTACT_CHANNEL, B.Region_Administrative, A.EVENT_DATE
from
(select  EVENT_DATE, MSISDN,SOS_TYPE,TRANSACTION_TYPE,MONTANT,COMMISSION,PRICE_PLAN_NAME ,CONTACT_CHANNEL
from
(select  original_file_date  EVENT_DATE,
     substring(msisdn,4,9)   MSISDN,
           'SOS_VOICE'  as SOS_TYPE,
                   TRANSACTION_TYPE,
                    PRICE_PLAN_CODE,
(case when TRANSACTION_TYPE = 'LOAN' then nvl(amount,0)/100 else  nvl(-1*amount,0)/100 end) MONTANT,
         nvl(fee,0)/100 COMMISSION,
         CONTACT_CHANNEL
from CDR.SPARK_IT_ZTE_LOAN_CDR
where original_file_date ='###SLICE_VALUE###'
) A left join
(select price_plan_name, price_plan_code
from cdr.spark_it_zte_price_plan_extract
where original_file_date ='###SLICE_VALUE###'
group by price_plan_name, price_plan_code) B
on A.price_plan_code = B.price_plan_code
UNION
select transaction_date EVENT_DATE, msisdn, 'SOS_DATA' as SOS_TYPE, TRANSACTION_TYPE ,
(case when TRANSACTION_TYPE = 'LOAN' then nvl(amount,0) else  nvl(-1*amount,0) end) MONTANT,
(case when transaction_type ='PAYBACK' then nvl(BYTES_OBTAINED,0)/100 else 0 end) COMMISSION,
'Not Assigned' AS  PRICE_PLAN_NAME,
CONTACT_CHANNEL
from mon.spark_ft_emergency_data
where transaction_date ='###SLICE_VALUE###'
UNION
select transaction_date as EVENT_DATE, served_party_msisdn MSISDN, 'SOS_CREDIT' as SOS_TYPE,
(case when operation_type ='REIMBURSMENT' then 'PAYBACK' else operation_type end) TRANSACTION_TYPE,
                               LOAN_AMOUNT AS  MONTANT ,
                                     FEE AS COMMISSION ,
                    'Not Assigned' AS  PRICE_PLAN_NAME ,
                                Null as CONTACT_CHANNEL
from mon.spark_ft_overdraft
where transaction_date ='###SLICE_VALUE###') A
left join
(select msisdn ,
(case  when upper(townname) ='DOUALA' then 'a.DOUALA'
      when upper(townname) ='YAOUNDE' then 'b.YAOUNDE'
      when upper(administrative_region)='LITTORAL' then 'c.LITTORAL'
      when upper(administrative_region)='CENTRE' then 'd.CENTRE'
      when upper(administrative_region)='EXTREME-NORD' then 'e.EXTREME-NORD'
      when upper(administrative_region)='NORD' then 'f.NORD'
      when upper(administrative_region)='ADAMAOUA' then 'g.ADAMAOUA'
      when upper(administrative_region)='NORD-OUEST' then 'h.NORD-OUEST'
      when upper(administrative_region)='SUD-OUEST' then 'i.SUD-OUEST'
      when upper(administrative_region)= 'OUEST' then 'j.OUEST'
      when upper(administrative_region)= 'SUD' then 'k.SUD'
else 'EST' end) Region_Administrative
from mon.spark_ft_client_last_site_day where event_date='###SLICE_VALUE###') B
on A.msisdn = B.msisdn