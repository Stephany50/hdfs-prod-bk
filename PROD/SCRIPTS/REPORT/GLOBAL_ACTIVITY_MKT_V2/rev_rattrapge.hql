

insert into AGG.SPARK_FT_A_PARC_OM2
select
    administrative_region,
    max(service_type) service_type,
    count(distinct t1.msisdn) parc_om_30j,
    max(details) details,
    max(OPERATOR_CODE) OPERATOR_CODE,
    max(PROFILE_CODE) PROFILE_CODE,
    current_timestamp insert_date,
    jour
from (
    SELECT
            max(service_type) service_type,
            msisdn,
            max(details) details,
            max(OPERATOR_CODE) OPERATOR_CODE,
            max(PROFILE_CODE) PROFILE_CODE,
            a.jour jour
    from (
        select datecode jour FROM dim.dt_dates where datecode between '2020-07-01' and '2020-11-29'
     )  A
    LEFT JOIN  (
        select
            service_type,
            msisdn,
            details,
            OPERATOR_CODE OPERATOR_CODE,
            PROFILE PROFILE_CODE,
            jour
        from (select * from MON.SPARK_DATAMART_OM_MARKETING2 where  JOUR>='2020-07-01' AND STYLE NOT LIKE ('REC%') ) a
        left join (
            SELECT
                event_date,
                A.ACCESS_KEY,
                max(PROFILE) PROFILE,
                MAX(OPERATOR_CODE) OPERATOR_CODE
            FROM MON.SPARK_FT_CONTRACT_SNAPSHOT A
            WHERE EVENT_DATE>='2020-07-01'
            GROUP BY A.ACCESS_KEY, EVENT_DATE
        ) b ON a.jour=b.event_date and nvl(b.ACCESS_KEY,'INCONNU') = nvl(a.msisdn,'INCONNU')
        group by
            service_type,
            msisdn,
            details,
            OPERATOR_CODE ,
            PROFILE ,
            jour
    ) b   on b.JOUR BETWEEN date_sub(a.jour,30) and a.jour
    group by
            msisdn,
            a.jour
) T1
LEFT JOIN (select MSISDN,administrative_region,event_date from mon.spark_ft_client_last_site_day where event_date>='2020-07-01' ) T2 ON T1.MSISDN=T2.MSISDN and T1.JOUR =T2.event_date
group by administrative_region,jour

