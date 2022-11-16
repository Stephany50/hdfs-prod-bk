create table tt.dd_test as

create table tt.dd_test2 as


create table tt.dd_test3 as
select
    SERVED_PARTY_MSISDN,
    msisdn msisdn_imei_new,
    identificateur,
    est_snappe,
    SITE_NAME,
    SITE_INFOS,
    ACTIVATION_MONTH,
    AMOUNT_DATA,
    Mega_O,
    INSERT_DATE,
    C2S_M,
    TRANSACTION_DATE
from
(
    select 
        SERVED_PARTY_MSISDN,
        msisdn msisdn_imei_new,
        SITE_NAME,
        SITE_INFOS,
        ACTIVATION_MONTH,
        AMOUNT_DATA,
        Mega_O,
        INSERT_DATE,
        C2S_M,
        TRANSACTION_DATE
    from
    (
        select 
            TRANSACTION_DATE,
            SERVED_PARTY_MSISDN,
            to_date(INSERT_DATE) INSERT_DATE ,
            SITE_NAME,
            SITE_INFOS,
            ACTIVATION_MONTH,
            cast(AMOUNT_DATA as decimal(17, 2) ) AMOUNT_DATA,
            cast(bytes_used/(1024*1024) as decimal(17, 2)) Mega_O
        from
        (
            select 
                TRANSACTION_DATE,
                SERVED_PARTY_MSISDN,
                INSERT_DATE,
                SITE_NAME,
                SITE_INFOS,
                ACTIVATION_MONTH,
                AMOUNT_DATA 
            from 
            (
                select 
                    TRANSACTION_DATE,
                    SERVED_PARTY_MSISDN,
                    INSERT_DATE,
                    '' SITE_NAME,
                    '' SITE_INFOS,
                    substr(TRANSACTION_DATE,1,7) ACTIVATION_MONTH 
                FROM MON.Spark_ft_subscription 
                where TRANSACTION_DATE>=date_add(last_day(add_months('2022-04-30', -1)), 1) and TRANSACTION_DATE<='2022-04-30' 
                and subscription_service like '%PPS%'
            ) a
            left join
            (
                select 
                    msisdn,
                    sum(amount_data) amount_data ,
                    substr(event_date,1,7) PERIODE 
                from mon.spark_FT_SUBSCRIPTION_MSISDN_DAY where event_date>=date_add(last_day(add_months('2022-04-30', -1)), 1) and event_date<='2022-04-30'
                group by msisdn,substr(event_date,1,7)
            )
            on SERVED_PARTY_MSISDN=msisdn
        ) a
        left join
        (
            select 
                msisdn,
                sum(nvl(bytes_sent,0) + nvl(bytes_received,0)) bytes_used,
                substr(event_date,1,7)
            from MON.Spark_FT_DATA_CONSO_MSISDN_DAY 
            where event_date>=date_add(last_day(add_months('2022-04-30', -1)), 1) and event_date<='2022-04-30'
            group by msisdn,substr(event_date,1,7)
        ) b
        on SERVED_PARTY_MSISDN=b.msisdn
    ) a
    left join 
    (
        select distinct msisdn msisdn 
        from
        (
            select 
                a.msisdn, 
                a.imei, 
                a.activation_date, 
                b.imei as imei_next
            FROM
            (
                select distinct msisdn msisdn,
                    substr(imei, 1, 14) imei,
                    date_add(last_day(add_months('2022-04-30', -1)), 1) activation_date
                from MON.SPARK_FT_IMEI_ONLINE
                where sdate>=date_add(last_day(add_months('2022-04-30', -1)), 1) and sdate <= '2022-04-30'
                and trim(IMEI) rlike '^\\d{14,16}$'
            ) as a
            left outer join
            (
                select distinct imei
                from Mon.Spark_FT_IMEI_TRAFFIC_MONTHLY 
                where smonth >= replace(substr(add_months('2022-04-30', -6), 1, 7), '-') and smonth <= replace(substr(add_months('2022-04-30', -1), 1, 7), '-')
                and trim(IMEI) rlike '^\\d{14,16}$'
            ) as b
            on a.imei = b.imei
        )
        where imei_next is null
    ) b on SERVED_PARTY_MSISDN=MSISDN
    left join
    (
        select RECEIVER_MSISDN,SUM(REFILL_AMOUNT) C2S_M 
        from MON.spark_ft_refill
        where refill_date>=date_add(last_day(add_months('2022-04-30', -1)), 1) and refill_date<='2022-04-30' and REFILL_MEAN='C2S'
        and TERMINATION_IND='200'
        group by RECEIVER_MSISDN
    ) b on a.SERVED_PARTY_MSISDN=b.RECEIVER_MSISDN
) T
left join DIM.SPARK_DT_BASE_IDENTIFICATION U 
on T.SERVED_PARTY_MSISDN = U.msisdn