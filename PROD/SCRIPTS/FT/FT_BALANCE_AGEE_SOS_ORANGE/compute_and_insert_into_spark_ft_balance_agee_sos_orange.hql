insert into MON.SPARK_FT_BALANCE_AGEE_SOS_ORANGE
select       msisdn,
             sos_type,
             region_administrative,
             (case when  ENCOURS = 0 then 'PAS ENDETTE' else 'ENDETTE' end) statut,
             Age,
             TO_DATE(CURRENT_TIMESTAMP) AS INSERT_DATE,
             ENCOURS    montant,
             '###SLICE_VALUE###' AS EVENT_DATE
from
(select             msisdn,
                     sos_type,
                     region_administrative,
                     sum(montant)  ENCOURS,
                     max(datediff(CURRENT_DATE(), event_date)) Age
from (select  msisdn,
              sos_type,
              transaction_type,
              region_administrative,
              sum(case when transaction_type ='LOAN' then montant*1.1 else -montant end) montant,
              max(event_date) event_date
      from mon.spark_ft_sos_orange_reports
      where event_date between '2021-09-21' and '###SLICE_VALUE###'
      group by  msisdn, sos_type, transaction_type,region_administrative) T
group by msisdn, sos_type,region_administrative) T1