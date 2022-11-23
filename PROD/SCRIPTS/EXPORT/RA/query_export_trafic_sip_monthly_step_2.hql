insert into tt.tmp_extraction_appel_b2b
select
    t.mois, 
    PARTNER,
    min(faisceaux) min_faisceaux, 
    max(faisceaux) max_faisceaux,
    TRANSACTION_DATE, 
    t.MSISDN, 
    old_called_number NUMERO_APPELE,
    destination RESEAU,
    case when destination = 'INTERNATIONAL' then c.ZONE  when DESTINATION like '%OCM%' then 'ON-NET' when destination in ('MTN', 'VIETTEL', 'CAMTEL') then 'OFF-NET' else destination end DESTINATION,
    sum(transaction_duration) TRANSACTION_DURATION, 
    count(*) NBRE_APPEL_UNIQUE
from (
    select distinct partner,
        faisceaux,
        transaction_date,
        transaction_time,
        old_calling_number msisdn,
        old_called_number,
        destination,
        transaction_duration,
        date_format(transaction_date, 'yyyy-MM') mois
    from  TT.TMP_TRUNCK_SIP_TRANSACT
)t 
left join (
    select mois, msisdn, b.cc, b.zone
    from ( 
        select distinct date_format(transaction_date, 'yyyy-MM') mois, old_called_number msisdn from TT.TMP_TRUNCK_SIP_TRANSACT where destination = 'INTERNATIONAL'
    ) a
    join dim.spark_dt_Ref_contry_code b on substr(a.msisdn,1,length(b.cc))=b.CC
) c on t.OLD_CALLED_NUMBER = c.msisdn and destination = 'INTERNATIONAL' and t.mois=c.mois
group by t.mois, PARTNER, transaction_date, t.msisdn, old_called_number, destination, c.zone
order by t.mois, partner, transaction_date

