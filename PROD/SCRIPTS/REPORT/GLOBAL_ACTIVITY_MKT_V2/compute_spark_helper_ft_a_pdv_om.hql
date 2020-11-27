


create table junk.pdv_om_30J as
select
    administrative_region,
    max(service_type) service_type,
    count(distinct t1.msisdn) parc_om_30j,
    max(style) style,
    max(OPERATOR_CODE) OPERATOR_CODE,
    max(PROFILE_CODE) PROFILE_CODE,
    jour
from (

    SELECT
            max(service_type) service_type,
            msisdn,
            max(style) style,
            max(OPERATOR_CODE) OPERATOR_CODE,
            max(PROFILE_CODE) PROFILE_CODE,
            jour
    from (
        select
            service_type,
            msisdn,
            style,
            OPERATOR_CODE,
            PROFILE_CODE,
            a.jour,
            potential_ref_date,
            ROW_NUMBER() OVER(PARTITION BY b.jour ORDER BY potential_ref_date  desc) ID

        FROM (
                select datecode jour,ref_date potential_ref_date FROM (select * from dim.dt_dates where datecode between '###SLICE_VALUE###' and '###SLICE_VALUE###' ) a
                left join (select distinct ref_date from MON.SPARK_REF_OM_PRODUCTS2  ) b on b.ref_date<=a.datecode
                group by datecode,ref_date

             )  A
            LEFT JOIN  (
                select
                    service_type,
                    msisdn,
                    style,
                    OPERATOR_CODE OPERATOR_CODE,
                    PROFILE PROFILE_CODE,
                    REF_DATE,
                    jour
                from (
                    select DOD.*,RE.REF_DATE FROM
                        MON.SPARK_DATAMART_OM_DISTRIB DOD
                    LEFT JOIN MON.SPARK_REF_OM_PRODUCTS2 RE ON DOD.MSISDN=RE.MSISDN
                    where  JOUR>=date_sub('###SLICE_VALUE###',30) AND   PRODUCT_LINE='DISTRIBUTION' AND SERVICE_TYPE NOT LIKE 'P2P%'
                ) a
                left join (
                    SELECT
                        event_date,
                        A.ACCESS_KEY,
                        max(PROFILE) PROFILE,
                        MAX(OPERATOR_CODE) OPERATOR_CODE
                    FROM MON.SPARK_FT_CONTRACT_SNAPSHOT A
                    WHERE EVENT_DATE>='###SLICE_VALUE###'
                    GROUP BY A.ACCESS_KEY, EVENT_DATE
                ) b ON a.jour=b.event_date and nvl(b.ACCESS_KEY,'INCONNU') = nvl(a.msisdn,'INCONNU')
                group by
                    service_type,
                    msisdn,
                    style,
                    OPERATOR_CODE ,
                    PROFILE ,
                    REF_DATE,
                    jour
            ) B   on b.JOUR BETWEEN date_sub(a.jour,30) and potential_ref_date=ref_date
        ) T where ID=1
           group by
            msisdn,
            jour
) T1
LEFT JOIN (select MSISDN,administrative_region,event_date from mon.spark_ft_client_last_site_day where event_date>='###SLICE_VALUE###' ) T2 ON T1.MSISDN=T2.MSISDN and T1.JOUR =T2.event_date
group by administrative_region,jour

