INSERT INTO AGG.SPARK_FT_INTERNATIONAL_CEMAC PARTITION(CALL_DATE)

SELECT
distinct IMSI,
CLIENT_OPERATOR,
DIRECTION,
ROAMING_PARTNER_NAME,
ROAMING_PARTNER_COUNTRY,
SERVICE_FAMILY,
NUMBER_OF_CALLS,
MINUTES,

CALL_DATE
FROM 
    (SELECT 
        B.transaction_date CALL_DATE,
        B.served_imsi IMSI,
        count(*) NUMBER_OF_CALLS,
        sum(nvl(B.transaction_duration,0))/60 MINUTES,
        IF(B.transaction_direction='Sortant','Voice-MOC_CEMAC',
            IF(B.transaction_direction='Sortant' AND SUBSTR (B.other_party, 1, 3)=SUBSTR (B.partner_gt, 1, 3), 'Voice-partner_gt',
                IF( B.transaction_direction='Sortant' AND SUBSTR (B.other_party, 1, 3)='237','Voice-MOC_BH',
                    IF(B.transaction_direction='Entrant', 'Voice-MTC_CEMAC','Voice-MTC')
                )
            )
        )SERVICE_FAMILY,
        D.code_operateur CLIENT_OPERATOR,
        D.operateur ROAMING_PARTNER_NAME, 
        (CASE
            WHEN B.transaction_direction = 'Sortant' THEN 'ROAMING SORTANT'
            ELSE 'ROAMING ENTRANT' 
        END) DIRECTION,
        trim(D.country_name) as ROAMING_PARTNER_COUNTRY
    FROM MON.SPARK_FT_MSC_TRANSACTION B
    INNER JOIN (select * from DIM.SPARK_DT_REF_OPERATEURS where cc in (235,236,237,240,241,243)  )D 
    ON SUBSTR (B.other_party, 1, 3) = trim(D.cc)
    WHERE B.transaction_date = '###SLICE_VALUE###' AND B.transaction_type like "%TEL%"
    group by 
        IMSI,TRANSACTION_DATE,DIRECTION,
        ROAMING_PARTNER_COUNTRY,
        CLIENT_OPERATOR,ROAMING_PARTNER_NAME,SERVICE_FAMILY ) R1


