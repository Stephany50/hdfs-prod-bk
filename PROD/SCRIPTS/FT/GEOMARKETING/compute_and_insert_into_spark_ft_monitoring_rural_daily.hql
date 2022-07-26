INSERT INTO MON.MONITORING_RURAL_DAILY PARTITION(event_date)
select 
    COALESCE(T.site_name, G.site_name, H.site_name, A.site_name, B.site_name, C.site_name, D.site_name, E.site_name, F.site_name, J.site_name, K.site_name,
    L.site_name, M.site_name, N.site_name, O.site_name, P.site_name, Q.site_name) site,
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
    '###SLICE_VALUE###' event_date
from
(
    select
        distinct site_name,
        event_date
    from MON.SPARK_FT_GEOMARKETING_REPORT_360
    where event_date = '###SLICE_VALUE###'
) T

full join
(
    select 
        distinct loc_site_name site_name,
        gross_add,
        parc_actif_om
    from mon.spark_ft_site_360
    where event_date = '###SLICE_VALUE###'
) A
on upper(trim(T.site_name)) = upper(trim(A.site_name))

full join
(
    select
        distinct site_name,
        kpi_value RECHARGES
    from MON.SPARK_FT_GEOMARKETING_REPORT_360
    where event_date = '###SLICE_VALUE###' and kpi_name='RECHARGES'
) B
on upper(trim(T.site_name)) = upper(trim(B.site_name))

full join
(
    select
        distinct site_name,
        kpi_value CASHOUT
    from MON.SPARK_FT_GEOMARKETING_REPORT_360
    where event_date = '###SLICE_VALUE###' and kpi_name='CASHOUT'
) C
on upper(trim(T.site_name)) = upper(trim(C.site_name))

full join
(
    select
        distinct site_name,
        kpi_value CASHIN
    from MON.SPARK_FT_GEOMARKETING_REPORT_360
    where event_date = '###SLICE_VALUE###' and kpi_name='CASHIN'
) D
on upper(trim(T.site_name)) = upper(trim(D.site_name))

full join
(
    select
        distinct site_name,
        kpi_value CALL_BOX
    from MON.SPARK_FT_GEOMARKETING_REPORT_360
    where event_date = '###SLICE_VALUE###' and kpi_name='CALL_BOX'
) E
on upper(trim(T.site_name)) = upper(trim(E.site_name))

full join
(
    select
        distinct site_name,
        kpi_value POS_OM
    from MON.SPARK_FT_GEOMARKETING_REPORT_360
    where event_date = '###SLICE_VALUE###' and kpi_name='POS_OM'
) F
on upper(trim(T.site_name)) = upper(trim(F.site_name))

full join
(
    select
        distinct site_name,
        kpi_value PARC_ART
    from MON.SPARK_FT_GEOMARKETING_REPORT_360
    where event_date = '###SLICE_VALUE###' and kpi_name='PARC_ART'
) G
on upper(trim(T.site_name)) = upper(trim(G.site_name))

full join
(
    select
        distinct site_name,
        kpi_value PARC_GROUPE
    from MON.SPARK_FT_GEOMARKETING_REPORT_360
    where event_date = '###SLICE_VALUE###' and kpi_name='PARC_GROUPE'
) H
on upper(trim(T.site_name)) = upper(trim(H.site_name))

full join
(
    select
        distinct site_name,
        kpi_value DATAUSERS
    from MON.SPARK_FT_GEOMARKETING_REPORT_360
    where event_date = '###SLICE_VALUE###' and kpi_name='DATAUSERS'
) J
on upper(trim(T.site_name)) = upper(trim(J.site_name))

full join
(
    select
        distinct site_name,
        kpi_value NBRE_DEVICE_2G
    from MON.SPARK_FT_GEOMARKETING_REPORT_360
    where event_date = '###SLICE_VALUE###' and kpi_name='NBRE_DEVICE_2G'
) K
on upper(trim(T.site_name)) = upper(trim(K.site_name))

full join
(
    select
        distinct site_name,
        kpi_value NBRE_DEVICE_3G
    from MON.SPARK_FT_GEOMARKETING_REPORT_360
    where event_date = '###SLICE_VALUE###' and kpi_name='NBRE_DEVICE_3G'
) L
on upper(trim(T.site_name)) = upper(trim(L.site_name))

full join
(
    select
        distinct site_name,
        kpi_value NBRE_DEVICE_4G
    from MON.SPARK_FT_GEOMARKETING_REPORT_360
    where event_date = '###SLICE_VALUE###' and kpi_name='NBRE_DEVICE_4G'
) M
on upper(trim(T.site_name)) = upper(trim(M.site_name))

full join
(
    select
        distinct site_name,
        kpi_value NBRE_DEVICE_5G
    from MON.SPARK_FT_GEOMARKETING_REPORT_360
    where event_date = '###SLICE_VALUE###' and kpi_name='NBRE_DEVICE_5G'
) N
on upper(trim(T.site_name)) = upper(trim(N.site_name))

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
                WHERE TRANSACTION_DATE  = '###SLICE_VALUE###' 
                AND SUBSCRIPTION_SERVICE LIKE '%PPS%' 
            ) N
            inner join 
            (
                SELECT 
                    served_party_msisdn MSISDN,
                    (sum(bytes_sent) + sum(bytes_received)) /1024/1024 data_used
                from MON.SPARK_FT_CRA_GPRS
                where session_date = '###SLICE_VALUE###'
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
            WHERE EVENT_DATE = '###SLICE_VALUE###'
            GROUP BY MSISDN
        ) F10
        FULL JOIN
        (
            SELECT
                MSISDN,
                MAX(SITE_NAME) SITE_NAME
            FROM MON.SPARK_FT_CLIENT_SITE_TRAFFIC_DAY
            WHERE EVENT_DATE = '###SLICE_VALUE###'
            GROUP BY MSISDN
        ) F11
        ON F10.MSISDN = F11.MSISDN
    ) L
    on A.MSISDN = L.MSISDN
    group by SITE_NAME  
) O
on upper(trim(T.site_name)) = upper(trim(O.site_name))

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
            where event_date = date_add('###SLICE_VALUE###', 1)
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
            WHERE EVENT_DATE = '###SLICE_VALUE###'
            GROUP BY MSISDN
        ) F10
        FULL JOIN
        (
            SELECT
                MSISDN,
                MAX(SITE_NAME) SITE_NAME
            FROM MON.SPARK_FT_CLIENT_SITE_TRAFFIC_DAY
            WHERE EVENT_DATE = '###SLICE_VALUE###'
            GROUP BY MSISDN
        ) F11
        ON F10.MSISDN = F11.MSISDN
    ) L
    on G.MSISDN = L.MSISDN
    group by SITE_NAME
) P
on upper(trim(T.site_name)) = upper(trim(P.site_name))

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
                WHERE TRANSACTION_DATE  = '###SLICE_VALUE###'
                AND SUBSCRIPTION_SERVICE LIKE '%PPS%' 
            ) N
            inner join 
            (
                SELECT distinct
                    sender_msisdn msisdn
                FROM cdr.spark_it_omny_transactions
                WHERE transfer_datetime  = '###SLICE_VALUE###'

                UNION

                SELECT distinct
                    receiver_msisdn msisdn
                FROM cdr.spark_it_omny_transactions
                WHERE transfer_datetime  = '###SLICE_VALUE###'
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
            WHERE EVENT_DATE = '###SLICE_VALUE###'
            GROUP BY MSISDN
        ) F10
        FULL JOIN
        (
            SELECT
                MSISDN,
                MAX(SITE_NAME) SITE_NAME
            FROM MON.SPARK_FT_CLIENT_SITE_TRAFFIC_DAY
            WHERE EVENT_DATE = '###SLICE_VALUE###'
            GROUP BY MSISDN
        ) F11
        ON F10.MSISDN = F11.MSISDN
    ) L
    on A.MSISDN = L.MSISDN
    group by SITE_NAME
) Q
on upper(trim(T.site_name)) = upper(trim(Q.site_name))