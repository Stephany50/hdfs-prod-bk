insert into mon.spark_ft_msisdn_bal_usage_hour
select
    msisdn
    , substr(SESSION_TIME, 1, 2) hour_period
    , bal_id
    , ACCT_RES_ID
    , ACCT_RES_NAME
    , ACCT_RES_RATING_TYPE
    , ACCT_RES_RATING_UNIT
    , sum(used_volume) used_volume
    , current_timestamp insert_date
    , '2021-04-11' event_date
from
(
    select
        msisdn
        , SESSION_TIME
        , (
            case
                when bal_number = 'bal_id1' then bal_id1
                when bal_number = 'bal_id2' then bal_id2
                when bal_number = 'bal_id3' then bal_id3
                when bal_number = 'bal_id4' then bal_id4
            end
        ) bal_id
        , (
            case
                when bal_number = 'bal_id1' then charge1_corrected
                when bal_number = 'bal_id2' then charge2_corrected
                when bal_number = 'bal_id3' then charge3_corrected
                when bal_number = 'bal_id4' then charge4_corrected
            end
        ) used_volume
        , (
            case
                when bal_number = 'bal_id1' then ACCT_ITEM_TYPE_ID1
                when bal_number = 'bal_id2' then ACCT_ITEM_TYPE_ID2
                when bal_number = 'bal_id3' then ACCT_ITEM_TYPE_ID3
                when bal_number = 'bal_id4' then ACCT_ITEM_TYPE_ID4
            end
        ) ACCT_ITEM_TYPE_ID
    from
    (
        select 'bal_id1' as bal_number
        union select 'bal_id2' as bal_number
        union select 'bal_id3' as bal_number
        union select 'bal_id4' as bal_number
    ) a0,
    (
        SELECT
            SUBSTRING(calling_nbr,-9) msisdn
            , DATE_FORMAT(start_time,'HHmmss') SESSION_TIME
            , ACCT_ITEM_TYPE_ID1
            , ACCT_ITEM_TYPE_ID2
            , ACCT_ITEM_TYPE_ID3
            , ACCT_ITEM_TYPE_ID4
            , bal_id1
            , bal_id2
            , bal_id3
            , bal_id4
            , IF(charge1 < 0, 0, NVL(charge1,0)) charge1_corrected
            , IF(charge2 < 0, 0, NVL(charge2,0)) charge2_corrected
            , IF(charge3 < 0, 0, NVL(charge3,0)) charge3_corrected
            , IF(charge4 < 0, 0, NVL(charge4,0)) charge4_corrected
        FROM CDR.SPARK_IT_ZTE_DATA
        WHERE START_DATE = '2021-04-11'
        union all
        SELECT
            FN_GET_NNP_MSISDN_9DIGITS(CALLING_NBR) msisdn
            , DATE_FORMAT(start_time,'HHmmss') SESSION_TIME
            , ACCT_ITEM_TYPE_ID1
            , ACCT_ITEM_TYPE_ID2
            , ACCT_ITEM_TYPE_ID3
            , ACCT_ITEM_TYPE_ID4
            , bal_id1
            , bal_id2
            , bal_id3
            , bal_id4
            , IF(charge1 < 0, 0, NVL(charge1,0)) charge1_corrected
            , IF(charge2 < 0, 0, NVL(charge2,0)) charge2_corrected
            , IF(charge3 < 0, 0, NVL(charge3,0)) charge3_corrected
            , IF(charge4 < 0, 0, NVL(charge4,0)) charge4_corrected
        FROM CDR.SPARK_IT_ZTE_VOICE_SMS
        WHERE start_date = '2021-04-11'
    ) a1
) a
LEFT JOIN
(
    SELECT
        ACCT_RES_ID
        , ACCT_ITEM_TYPE_ID
        , UPPER(ACCT_RES_NAME) ACCT_RES_NAME
        , ACCT_RES_RATING_TYPE
        , ACCT_RES_RATING_UNIT
    FROM DIM.DT_BALANCE_TYPE_ITEM
) b ON a.ACCT_ITEM_TYPE_ID = b.ACCT_ITEM_TYPE_ID
where bal_id is not null and acct_res_id != 1
group by msisdn
    , substr(SESSION_TIME, 1, 2)
    , bal_id
    , ACCT_RES_ID
    , ACCT_RES_NAME
    , ACCT_RES_RATING_TYPE
    , ACCT_RES_RATING_UNIT
