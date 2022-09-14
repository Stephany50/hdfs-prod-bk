INSERT INTO MON.SPARK_FT_MONITORING_RURAL_MONTHLY PARTITION(event_month)
select 
    COALESCE(T.site_name, G.site_name, H.site_name, A.site_name, B.site_name, C.site_name, D.site_name, E.site_name, F.site_name, J.site_name, K.site_name,
    L.site_name, M.site_name, N.site_name, O.site_name, P.site_name, Q.site_name, R.site_name) site,
    gross_add,
    parc_actif_om,
    RECHARGES,
    CASHOUT,
    CASHIN,
    CALL_BOX,
    POS_OM,
    PARC_ART,
    PARC_GROUPE,
    DATAUSERS,
    NBRE_DEVICE_2G,
    NBRE_DEVICE_3G,
    NBRE_DEVICE_4G,
    NBRE_DEVICE_5G,
    gross_add_data,
    charged_base,
    gross_add_om,
    CURRENT_TIMESTAMP INSERT_DATE,
    '###SLICE_VALUE###' event_month
from
(
    select
        distinct site_name
    from MON.SPARK_FT_GEOMARKETING_REPORT_360
    where to_date(EVENT_DATE) BETWEEN to_date(concat("###SLICE_VALUE###",'-01'))  AND to_date(last_day(concat("###SLICE_VALUE###",'-01')))
) T

full join
(
    select 
        site_name,
        max(kpi_value) PARC_ACTIF_OM
    from MON.SPARK_FT_GEOMARKETING_REPORT_360_MTD
    where to_date(EVENT_DATE) = to_date(last_day(concat("###SLICE_VALUE###",'-01'))) and kpi_name='PARC_ACTIF_OM'
    group by site_name
) A
on upper(trim(T.site_name)) = upper(trim(A.site_name))

full join
(
    select 
        site_name,
        max(kpi_value) GROSS_ADD
    from MON.SPARK_FT_GEOMARKETING_REPORT_360_MTD
    where to_date(EVENT_DATE) = to_date(last_day(concat("###SLICE_VALUE###",'-01'))) and kpi_name='GROSS_ADD'
    group by site_name
) B
on upper(trim(T.site_name)) = upper(trim(B.site_name))


full join
(
    select
        site_name,
        max(kpi_value) RECHARGES
    from MON.SPARK_FT_GEOMARKETING_REPORT_360_MTD
    where to_date(EVENT_DATE) = to_date(last_day(concat("###SLICE_VALUE###",'-01'))) and kpi_name='RECHARGES'
    group by site_name
) C
on upper(trim(T.site_name)) = upper(trim(C.site_name))

full join
(
    select
        site_name,
        max(kpi_value) CASHOUT
    from MON.SPARK_FT_GEOMARKETING_REPORT_360_MTD
    where to_date(EVENT_DATE) = to_date(last_day(concat("###SLICE_VALUE###",'-01'))) and kpi_name='CASHOUT'
    group by site_name
) D
on upper(trim(T.site_name)) = upper(trim(D.site_name))

full join
(
    select
        site_name,
        max(kpi_value) CASHIN
    from MON.SPARK_FT_GEOMARKETING_REPORT_360_MTD
    where to_date(EVENT_DATE) = to_date(last_day(concat("###SLICE_VALUE###",'-01'))) and kpi_name='CASHIN'
    group by site_name
) E
on upper(trim(T.site_name)) = upper(trim(E.site_name))

full join
(
    select
        site_name,
        max(kpi_value) CALL_BOX
    from MON.SPARK_FT_GEOMARKETING_REPORT_360_MTD
    where to_date(EVENT_DATE) = to_date(last_day(concat("###SLICE_VALUE###",'-01'))) and kpi_name='CALL_BOX'
    group by site_name
) F
on upper(trim(T.site_name)) = upper(trim(F.site_name))

full join
(
    select
        site_name,
        max(kpi_value) POS_OM
    from MON.SPARK_FT_GEOMARKETING_REPORT_360_MTD
    where to_date(EVENT_DATE) = to_date(last_day(concat("###SLICE_VALUE###",'-01'))) and kpi_name='POS_OM'
    group by site_name
) G
on upper(trim(T.site_name)) = upper(trim(G.site_name))

full join
(
    select
        site_name,
        max(kpi_value) PARC_ART
    from MON.SPARK_FT_GEOMARKETING_REPORT_360_MTD
    where to_date(EVENT_DATE) = to_date(last_day(concat("###SLICE_VALUE###",'-01'))) and kpi_name='PARC_ART'
    group by site_name
) H
on upper(trim(T.site_name)) = upper(trim(H.site_name))

full join
(
    select
        site_name,
        max(kpi_value) PARC_GROUPE
    from MON.SPARK_FT_GEOMARKETING_REPORT_360_MTD
    where to_date(EVENT_DATE) = to_date(last_day(concat("###SLICE_VALUE###",'-01'))) and kpi_name='PARC_GROUPE'
    group by site_name
) J
on upper(trim(T.site_name)) = upper(trim(J.site_name))

full join
(
    select
        site_name,
        max(kpi_value) DATAUSERS
    from MON.SPARK_FT_GEOMARKETING_REPORT_360_MTD
    where to_date(EVENT_DATE) = to_date(last_day(concat("###SLICE_VALUE###",'-01'))) and kpi_name='DATAUSERS'
    group by site_name
) K
on upper(trim(T.site_name)) = upper(trim(K.site_name))

full join
(
    select
        site_name,
        max(kpi_value) NBRE_DEVICE_2G
    from MON.SPARK_FT_GEOMARKETING_REPORT_360_MTD
    where to_date(EVENT_DATE) = to_date(last_day(concat("###SLICE_VALUE###",'-01'))) and kpi_name='NBRE_DEVICE_2G'
    group by site_name
) L
on upper(trim(T.site_name)) = upper(trim(L.site_name))

full join
(
    select
        site_name,
        max(kpi_value) NBRE_DEVICE_3G
    from MON.SPARK_FT_GEOMARKETING_REPORT_360_MTD
    where to_date(EVENT_DATE) = to_date(last_day(concat("###SLICE_VALUE###",'-01'))) and kpi_name='NBRE_DEVICE_3G'
    group by site_name
) M
on upper(trim(T.site_name)) = upper(trim(M.site_name))

full join
(
    select
        site_name,
        max(kpi_value) NBRE_DEVICE_4G
    from MON.SPARK_FT_GEOMARKETING_REPORT_360_MTD
    where to_date(EVENT_DATE) = to_date(last_day(concat("###SLICE_VALUE###",'-01'))) and kpi_name='NBRE_DEVICE_4G'
    group by site_name
) N
on upper(trim(T.site_name)) = upper(trim(N.site_name))

full join
(
    select
        site_name,
        max(kpi_value) NBRE_DEVICE_5G
    from MON.SPARK_FT_GEOMARKETING_REPORT_360_MTD
    where to_date(EVENT_DATE) = to_date(last_day(concat("###SLICE_VALUE###",'-01'))) and kpi_name='NBRE_DEVICE_5G'
    group by site_name
) O
on upper(trim(T.site_name)) = upper(trim(O.site_name))

full join
(
    select 
        SITE_NAME, 
        count (distinct A.MSISDN) gross_add_data
    from 
    (
        select
            MSISDN  
        from
        (
            SELECT 
                N.MSISDN,
                case when nvl(data_used, 0) > 1 then 1 else 0 end tmp_gross_add_data
            from 
            (
                SELECT distinct
                    SERVED_PARTY_MSISDN msisdn
                FROM MON.SPARK_FT_SUBSCRIPTION
                WHERE to_date(TRANSACTION_DATE) BETWEEN to_date(concat("###SLICE_VALUE###",'-01'))  AND to_date(last_day(concat("###SLICE_VALUE###",'-01')))
                AND SUBSCRIPTION_SERVICE LIKE '%PPS%' 
            ) N
            inner join 
            (
                SELECT 
                    served_party_msisdn MSISDN,
                    (sum(bytes_sent) + sum(bytes_received)) /1024/1024 data_used
                from MON.SPARK_FT_CRA_GPRS
                where to_date(session_date) BETWEEN to_date(concat("###SLICE_VALUE###",'-01'))  AND to_date(last_day(concat("###SLICE_VALUE###",'-01')))
                group by served_party_msisdn
            ) M
            on N.MSISDN = M.MSISDN
        ) where tmp_gross_add_data = 1 
    ) A
    left join 
    (
        SELECT
            nvl(F10.MSISDN, F11.MSISDN) MSISDN,
            UPPER(NVL(F11.SITE_NAME, F10.SITE_NAME)) SITE_NAME
        FROM
        (
            SELECT
                MSISDN,
                MAX(SITE_NAME) SITE_NAME
            FROM MON.SPARK_FT_CLIENT_LAST_SITE_DAY
            WHERE to_date(EVENT_DATE) BETWEEN to_date(concat("###SLICE_VALUE###",'-01'))  AND to_date(last_day(concat("###SLICE_VALUE###",'-01')))
            GROUP BY MSISDN
        ) F10
        FULL JOIN
        (
            SELECT
                MSISDN,
                MAX(SITE_NAME) SITE_NAME
            FROM MON.SPARK_FT_CLIENT_SITE_TRAFFIC_DAY
            WHERE to_date(EVENT_DATE) BETWEEN to_date(concat("###SLICE_VALUE###",'-01'))  AND to_date(last_day(concat("###SLICE_VALUE###",'-01')))
            GROUP BY MSISDN
        ) F11
        ON F10.MSISDN = F11.MSISDN
    ) L
    on A.MSISDN = L.MSISDN
    group by SITE_NAME  
) P
on upper(trim(T.site_name)) = upper(trim(P.site_name))

full join
(
    select
        SITE_NAME,
        count(distinct G.msisdn) charged_base
    from
    (
        select 
            msisdn
        from
        (
            select 
                msisdn,
                max(case when OG_CALL >= DATE_SUB(event_date,31) or least(IC_CALL_4,IC_CALL_3,IC_CALL_2,IC_CALL_1) >= DATE_SUB(event_date,31) then 1 else 0 end) charged 
            from MON.SPARK_FT_ACCOUNT_ACTIVITY a
            where to_date(event_date) =  to_date(last_day(concat("###SLICE_VALUE###",'-01')))
            group by msisdn  
        ) where charged = 1 
    ) G
    left join 
    (
        SELECT
            nvl(F10.MSISDN, F11.MSISDN) MSISDN,
            UPPER(NVL(F11.SITE_NAME, F10.SITE_NAME)) SITE_NAME
        FROM
        (
            SELECT
                MSISDN,
                MAX(SITE_NAME) SITE_NAME
            FROM MON.SPARK_FT_CLIENT_LAST_SITE_DAY
            WHERE to_date(EVENT_DATE) BETWEEN to_date(concat("###SLICE_VALUE###",'-01'))  AND to_date(last_day(concat("###SLICE_VALUE###",'-01')))
            GROUP BY MSISDN
        ) F10
        FULL JOIN
        (
            SELECT
                MSISDN,
                MAX(SITE_NAME) SITE_NAME
            FROM MON.SPARK_FT_CLIENT_SITE_TRAFFIC_DAY
            WHERE to_date(EVENT_DATE) BETWEEN to_date(concat("###SLICE_VALUE###",'-01'))  AND to_date(last_day(concat("###SLICE_VALUE###",'-01')))
            GROUP BY MSISDN
        ) F11
        ON F10.MSISDN = F11.MSISDN
    ) L
    on G.MSISDN = L.MSISDN
    group by SITE_NAME
) Q
on upper(trim(T.site_name)) = upper(trim(Q.site_name))

full join
(
    select
        SITE_NAME,
        count(A.msisdn) gross_add_om
    from
    (
        select 
            msisdn
        from 
        (
            SELECT 
                N.MSISDN,
                case when m.MSISDN is not null then 1 else 0 end gaom
            from 
            (
                SELECT distinct
                    SERVED_PARTY_MSISDN msisdn
                    FROM MON.SPARK_FT_SUBSCRIPTION
                WHERE to_date(TRANSACTION_DATE) BETWEEN to_date(concat("###SLICE_VALUE###",'-01'))  AND to_date(last_day(concat("###SLICE_VALUE###",'-01')))
                AND SUBSCRIPTION_SERVICE LIKE '%PPS%' 
            ) N
            inner join 
            (
                SELECT distinct
                    sender_msisdn msisdn
                FROM cdr.spark_it_omny_transactions
                WHERE to_date(transfer_datetime) BETWEEN to_date(concat("###SLICE_VALUE###",'-01'))  AND to_date(last_day(concat("###SLICE_VALUE###",'-01')))

                UNION

                SELECT distinct
                    receiver_msisdn msisdn
                FROM cdr.spark_it_omny_transactions
                WHERE to_date(transfer_datetime) BETWEEN to_date(concat("###SLICE_VALUE###",'-01'))  AND to_date(last_day(concat("###SLICE_VALUE###",'-01')))
            ) M
            on N.MSISDN = M.MSISDN
        ) where gaom = 1
    ) A
    left join 
    (
        SELECT
            nvl(F10.MSISDN, F11.MSISDN) MSISDN,
            UPPER(NVL(F11.SITE_NAME, F10.SITE_NAME)) SITE_NAME
        FROM
        (
            SELECT
                MSISDN,
                MAX(SITE_NAME) SITE_NAME
            FROM MON.SPARK_FT_CLIENT_LAST_SITE_DAY
            WHERE to_date(EVENT_DATE) BETWEEN to_date(concat("###SLICE_VALUE###",'-01'))  AND to_date(last_day(concat("###SLICE_VALUE###",'-01')))
            GROUP BY MSISDN
        ) F10
        FULL JOIN
        (
            SELECT
                MSISDN,
                MAX(SITE_NAME) SITE_NAME
            FROM MON.SPARK_FT_CLIENT_SITE_TRAFFIC_DAY
            WHERE to_date(EVENT_DATE) BETWEEN to_date(concat("###SLICE_VALUE###",'-01'))  AND to_date(last_day(concat("###SLICE_VALUE###",'-01')))
            GROUP BY MSISDN
        ) F11
        ON F10.MSISDN = F11.MSISDN
    ) L
    on A.MSISDN = L.MSISDN
    group by SITE_NAME
) R
on upper(trim(T.site_name)) = upper(trim(R.site_name))