insert into MON.SPARK_FT_LITE_GLOBAL_REFILL
      select

        case when ( SENDER_CATEGORY IN ('TN','TNT') and refill_type = 'RC')
                    or ( SENDER_CATEGORY IN ('WHA')  and refill_type = 'RC') then 'NOT_USED_RECEIVER'
                else SENDER_CATEGORY||'_'||refill_type
           end CATEGORY
        , sum(refill_amount) refill_amount   --sum(msisdn_count) msisdn_count
        , 'RS' Source_type
        , sum(msisdn_count) msisdn_count
        ,CURRENT_TIMESTAMP AS INSERT_DATE
        ,'' refill_date
        from
        (
        select refill_date, sender_category, refill_type, sum(refill_amount) refill_amount,count(distinct sender_msisdn) msisdn_count
            from
            (
            select SENDER_MSISDN, refill_date, SENDER_CATEGORY, refill_type, sum(refill_amount) refill_amount
            from MON.SPARK_ft_refill
            where refill_date = '2020-04-05'   --'13/03/2020' ----'01/11/2019'-- and refill_date <= '31/05/2018'
                   AND REFILL_MEAN ='C2S'
                   AND REFILL_TYPE  in ('RC', 'PVAS')
                   --AND SENDER_CATEGORY IN ('INHSM','INSM','NPOS','ORNGPTNR','PPOS')
                   and termination_ind = '200'
                   group by SENDER_MSISDN, refill_date, SENDER_CATEGORY, refill_type
            ) T
            group by  refill_date, sender_category, refill_type
        ) a
        group by refill_date, case when ( SENDER_CATEGORY IN ('TN','TNT') and refill_type = 'RC')
                    or ( SENDER_CATEGORY IN ('WHA')  and refill_type = 'RC') then 'NOT_USED_RECEIVER'
                else SENDER_CATEGORY||'_'||refill_type END