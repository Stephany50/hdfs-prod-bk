

pre requis
    1) spark_FT_OTARIE_DATA_TRAFFIC_DAY
    2) spark_ft_otarie_data_traffic_day
        s_sql_query := 'truncate table TT_MSISDN_DATA_SITE'; 


1) les données de IMEI_DATA_LOCATION


        
insert into TMP.TT_MSISDN_DATA_SITE
select distinct
    session_date,
    msisdn,
    first_value(imei)over (partition by msisdn order by bytes_used desc) imei,
    first_value(site_name)over(partition by msisdn order by bytes_used desc) site_name,
    sum(Bytes_used)over(partition by msisdn) Bytes_Used,
    sum(total_cost)over(partition by msisdn) total_cost,
    sum(bundle_volume_used)over(partition by msisdn) bundle_volume_used,
    CURRENT_TIMESTAMP insert_date
from
(
    select
        session_date,
        served_party_msisdn msisdn,
        substr(served_party_imei, 1, 14) imei,
        site_name,
        sum(bytes_sent+bytes_Received) Bytes_Used,
        sum(total_cost)total_cost,
        sum(main_cost) Main_cost,
        sum(promo_cost) Promo_cost,
        sum(bundle_bytes_used_volume)bundle_Volume_used,
        sum(total_count) transaction_count,
        sum(rated_count) rated_count
    from (
        select * from  MON.SPARK_FT_MSISDN_IMEI_DATA_LOCATION  where session_date='2020-04-01'
    ) imei
    left join (
        select cast(ci as STRING) as ci, site_name from DEFAULT.VW_SDT_CI_INFO_NEW
    )  site  on lpad(imei.location_ci,5,0) = lpad(site.ci,5,0)
    group by
        session_date,
        served_party_msisdn,
        substr(served_party_imei, 1, 14),
        site_name
)T  ;



2) les données otaries


IF n_is_otarie_done = 1 THEN
merge into TT_MSISDN_DATA_SITE  Dest
using
(
select
    TRANSACTION_DATE,
    MSISDN
    , sum(case when RADIO_ACCESS_TECHNO = '2G'
            then Nbytest else 0
            end) Otarie_BYTES_2G
    , sum(case when RADIO_ACCESS_TECHNO in ('3G', 'HSPA') then Nbytest else 0 end) Otarie_Bytes_3G
    , sum(case when RADIO_ACCESS_TECHNO = 'LTE' then Nbytest else 0 end) Otarie_Bytes_4G
    , sum(case when RADIO_ACCESS_TECHNO = 'Unknown' then Nbytest else 0 end) Otarie_Bytes_Ukn
from MON.SPARK_FT_OTARIE_DATA_TRAFFIC_DAY
where transaction_date = '2020-04-01'
group by  TRANSACTION_DATE, MSISDN
) Src
on ( Dest.msisdn = Src.msisdn
and session_date = d_slice_value )--'02/11/2016')
when matched then
    update set Dest.Otarie_Bytes_2G = Src.Otarie_Bytes_2G
        , Dest.Otarie_Bytes_3G = Src.Otarie_Bytes_3G
        , Dest.Otarie_Bytes_4G = Src.Otarie_Bytes_4G
        , Dest.Otarie_Bytes_Ukn = Src.Otarie_Bytes_Ukn
 when not matched then
            insert (session_date, msisdn, otarie_bytes_2G, otarie_bytes_3G, otarie_bytes_4G, otarie_bytes_Ukn)
                values(transaction_date, Src.msisdn, Src.otarie_bytes_2G, Src.otarie_bytes_3G, Src.otarie_bytes_4G, Src.otarie_bytes_Ukn);
        commit;

 END IF;
                    
 -- insertion des donnees dans la table            
        
    insert into FT_MSISDN_TRAFIC_DATA_SITE (event_date, msisdn, imei, site_name, Bytes_used, PayAsYouGo, Bundle_Volume_Used, Source, insert_date)
    select session_date, msisdn, imei, site_name, bytes_used, total_cost, bundle_volume_used, 'DATA' Source, sysdate
        from TT_MSISDN_DATA_SITE a
        where session_date = d_slice_value ; 
                
    commit;

    merge into FT_MSISDN_TRAFIC_DATA_SITE D
    using 
    (
        select event_date, a.msisdn, imei, site_name, duree_sortant duree_tel_sortant, duree_entrant duree_tel_entrant, nbre_tel_sortant, nbre_tel_entrant, nbre_sms_sortant
        , nbre_sms_entrant
        from
        (
            select * -- count(*)
            from FT_CLIENT_SITE_TRAFFIC_DAY
            where event_date = d_slice_value
        )  a
        , 
        (
            select * 
            from
            (
                select sdate, msisdn, imei, row_number()over(partition by msisdn order by transaction_count desc) rang
                from ft_imei_online
                where sdate = d_slice_value
            )
            where rang = 1
        ) b
        where a.msisdn =b.msisdn    
    ) S
    on ( D.event_date = S.event_date
        and D.msisdn = S.msisdn 
        )
    when matched then
        update set D.IMEI= nvl(D.imei, S.imei)
            , D.Site_name = nvl(D.Site_name, S.site_name)
            , D.duree_tel_entrant = S.duree_tel_entrant
            , D.duree_tel_sortant = S.duree_tel_sortant
            , D.nbre_tel_entrant = S.nbre_tel_entrant
            , D.nbre_tel_sortant = S.nbre_tel_sortant   
            , D.nbre_sms_entrant = S.nbre_sms_entrant
            , D.nbre_sms_sortant = S.nbre_sms_sortant
    when not matched then
        insert (event_date, msisdn, imei, site_name, duree_tel_Entrant, Duree_tel_sortant, Nbre_tel_entrant, nbre_tel_Sortant, nbre_sms_entrant, nbre_sms_sortant, source, insert_date)
        values ( S.event_date, S.msisdn, S.imei, S.site_name, S.duree_tel_Entrant, S.Duree_tel_sortant, S.Nbre_tel_entrant, S.nbre_tel_Sortant, S.nbre_sms_entrant, S.nbre_sms_sortant, 'Voix/SMS', sysdate)
    ;
            
    commit;
            
  --
 COMMIT;
 -- fin
 RETURN 'OK';


 END; -- end function

-- #END_REGION : FUNCTION FN_DO_FT_MSISDN_DATA_LOCATION
/
