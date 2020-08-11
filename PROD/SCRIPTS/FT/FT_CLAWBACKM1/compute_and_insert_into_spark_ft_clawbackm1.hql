INSERT INTO MON.SPARK_FT_CLAWBACKM1

SELECT
    IDENTIFIER_MSISDN,
    VOL_PAYE,
    REGUL_ENCEIGNE,
    (case when INF250='VRAI' then Class1 else 0 end) INF250,
    (case when INF300='VRAI' then Class1+Class2 else 0 end) INF300,
    (case when INF400='VRAI' then Class1+Class2+Class3 else 0 end) INF400,
    (case when INF450='VRAI' then Class1+Class2+Class3+Class4 else 0 end) INF450,
    current_date insert_date,
    '###SLICE_VALUE###' EVENT_DATE

from
    (
    select
        IDENTIFIER_MSISDN,
        VOL_PAYE,
        REGUL_ENCEIGNE,
        class1,
        class2,
        class3,
        class4,
        (CASE WHEN  (VOL_PAYE<=47) THEN 'VRAI' ELSE  'FAUX' END ) INF250,
        (CASE WHEN  (VOL_PAYE>47 and VOL_PAYE<=96) THEN 'VRAI' ELSE  'FAUX' END ) INF300,
        (CASE WHEN  (VOL_PAYE>96 and VOL_PAYE<=120) THEN 'VRAI' ELSE  'FAUX' END ) INF400,
        (CASE WHEN  (VOL_PAYE>120) THEN 'VRAI' ELSE  'FAUX' END ) INF450

    FROM
        (
        select
            EVENT_DATE,
            IDENTIFIER_MSISDN,
            count(MSISDN_IDENTIFIED) Vol_Paye,
            sum(case when Regul_enceigne='VRAI' then 1 else 0 end) Regul_enceigne,
            sum(case when Class1='VRAI' then 1 else 0 end) Class1,
            sum(case when Class2='VRAI' then 1 else 0 end) Class2,
            sum(case when Class3='VRAI' then 1 else 0 end) Class3,
            sum(case when Class4='VRAI' then 1 else 0 end) Class4,
            current_date insert_date

        from
            (
            select
            MSISDN_IDENTIFIED,
            IDENTIFIER_MSISDN,
            EVENT_DATE,
            (CASE WHEN  (C2S_M is not null) THEN 'VRAI' ELSE  'FAUX' END ) Regul_enceigne,
            (CASE WHEN  (CUMUL<=250) THEN 'VRAI' ELSE  'FAUX' END ) Class1,
            (CASE WHEN  (CUMUL>250 and CUMUL<=300) THEN 'VRAI' ELSE  'FAUX' END ) Class2,
            (CASE WHEN  (CUMUL>300 and CUMUL<=400) THEN 'VRAI' ELSE  'FAUX' END ) Class3,
            (CASE WHEN  (CUMUL>400 and CUMUL<=450) THEN 'VRAI' ELSE  'FAUX' END ) Class4

            FROM
                (
                select
                    '###SLICE_VALUE###' EVENT_DATE,
                    a.IDENTIFIER_MSISDN,
                    a.MSISDN_IDENTIFIED,
                    a.EST_ACTIVES,
                    a.EST_ACTIVES_SNAP_RECH_SUP_250,
                    a.RECHARGES_CUMULEES,C2S_M,
                    a.RECHARGES_CUMULEES+NVL(C2S_M,0) CUMUL

                from


                    (Select * from MON.SPARK_ACTIV_IDENTIF_DETAILS  where event_date = to_date(add_months(last_day('###SLICE_VALUE###'), -1)) and EST_ACTIVES_SNAP_RECH_SUP_250=1) a

                LEFT JOIN

                    (
                    Select
                        RECEIVER_MSISDN,
                        SUM(REFILL_AMOUNT) C2S_M,
                        DATE_FORMAT(refill_date,'yyyy/MM')

                    from MON.SPARK_FT_REFILL
                    where refill_date>='###SLICE_VALUE###' and refill_date<=last_day('###SLICE_VALUE###') and REFILL_MEAN='C2S' and TERMINATION_IND='200'
                    group by RECEIVER_MSISDN,DATE_FORMAT(refill_date,'yyyy/MM')
                    ) b

                on a.MSISDN_IDENTIFIED = b.RECEIVER_MSISDN
                ) a
            ) GG
        group by EVENT_DATE, IDENTIFIER_MSISDN
        ) a
    ) GGG









