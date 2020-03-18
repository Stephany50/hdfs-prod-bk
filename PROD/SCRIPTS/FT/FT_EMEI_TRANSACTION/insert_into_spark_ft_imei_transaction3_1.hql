Insert into TMP.SPARK_FT_IMEI_TRANSACTION_2
select
a.IMEI,
a.MSISDN,
a.site_name SITE_VOIX,
NULL ,
NULL,
a.TECHNOLOGIE,
b.TERMINAL_TYPE,
a.Nbre_Entrant NOMBRE_TRANSACTION_ENTRANT,
a.Nbre_Sortant  NOMBRE_TRANSACTION_SORTANT,
a.DUREE_ENTRANT,
a.DUREE_SORTANT,
NULL,
NULL,
NULL,
NULL,
NULL,
NULL,
NULL,
NULL,
NULL,
CURRENT_TIMESTAMP
'###SLICE_VALUE###' EVENT_DATE
from
        (select
        a.imei ,a.msisdn ,a.localisation ,a.Duree_Sortant ,a.Nbre_Sortant ,a.Duree_Entrant,a.Nbre_Entrant,b.site_name
        ,b.technologie
        from
        (select
        SERVED_MSISDN MSISDN,SERVED_IMEI IMEI,SERVED_PARTY_LOCATION LOCALISATION
        , sum(case when transaction_direction = 'Sortant' then Transaction_duration else 0 end) Duree_Sortant
        , sum(case when transaction_direction = 'Sortant' then 1 else 0 end) Nbre_Sortant
        , sum(case when transaction_direction = 'Entrant' then Transaction_duration else 0 end) Duree_Entrant
        , sum(case when transaction_direction = 'Entrant' then 1 else 0 end) Nbre_Entrant
        from  ft_msc_transaction where transaction_date ='###SLICE_VALUE###'
        group by SERVED_MSISDN,SERVED_IMEI,SERVED_PARTY_LOCATION) a

    LEFT JOIN

        (select (Case when length(CI) =3 then concat('00',CI)
        when length(CI) =4 then concat('0',CI)
        else cast(CI as string) end) CI,max(site_name) site_name, max(technologie) technologie
        from dim.dt_gsm_cell_code
        group by Case when length(CI) =3 then concat('00',CI)
        when length(CI) =4 then concat('0',CI)
        else cast(CI as string) end
        ) b
    ON substr(a.localisation,14,5) = b.CI
) a

LEFT JOIN

(select * from dim.dt_handset_ref) b

ON substr(a.imei,1,8) = b.TAC_CODE;

