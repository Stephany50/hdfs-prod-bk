 insert into AGG.SPARK_FT_A_REFILL_REVENUE
            select
                  REFILL_TYPE
                , REFILL_MEAN
                , SENDER_PROFILE
                , RECEIVER_PROFILE
                , sum(REFILL_AMOUNT) REFILL_AMOUNT
                , sum(REFILL_BONUS) REFILL_BONUS
                , count(*) REFILL_COUNT
                , CURRENT_TIMESTAMP last_update
                , SENDER_OPERATOR_CODE
                , RECEIVER_OPERATOR_CODE
                ,CURRENT_TIMESTAMP INSERT_DATE
                ,REFILL_DATE
            from MON.SPARK_FT_REFILL
            where REFILL_DATE = '###SLICE_VALUE###'
            and TERMINATION_IND='200'
            and REFILL_MEAN<>'SCRATCH'
            group by
               REFILL_DATE
                , REFILL_TYPE
                , REFILL_MEAN
                , SENDER_PROFILE
                , RECEIVER_PROFILE
                , SENDER_OPERATOR_CODE
                , RECEIVER_OPERATOR_CODE
        UNION ALL
            select

                  REFILL_TYPE
                , REFILL_MEAN
                , SENDER_PROFILE
                , RECEIVER_PROFILE
                , sum(REFILL_AMOUNT) REFILL_AMOUNT
                , sum(REFILL_BONUS) REFILL_BONUS

                , count(*) REFILL_COUNT

                , CURRENT_TIMESTAMP last_update
                , SENDER_OPERATOR_CODE
                , RECEIVER_OPERATOR_CODE
                , CURRENT_TIMESTAMP CURRENT_TIMESTAMP
                ,REFILL_DATE
            from MON.SPARK_FT_REFILL
            where REFILL_DATE = '###SLICE_VALUE###'
            and TERMINATION_IND='200'
            and REFILL_MEAN='SCRATCH'
            group by
               REFILL_DATE
                , REFILL_TYPE
                , REFILL_MEAN

                , SENDER_PROFILE
                , RECEIVER_PROFILE
                , SENDER_OPERATOR_CODE
                , RECEIVER_OPERATOR_CODE