INSERT INTO MON.SPARK_FT_ROAMING_CEMAC_IN_OUT PARTITION(CALL_DATE)

SELECT
distinct IMSI,
SERVED,
OTHER,
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
        concat(A.location_mcc,"",A.location_mnc,"",A.CHARGED_PARTY)IMSI,
        B.other_party OTHER,
         A.CHARGED_PARTY SERVED,
        count(*) NUMBER_OF_CALLS,
        sum(nvl(B.transaction_duration,0))/60 MINUTES,
        B.transaction_type SERVICE_FAMILY,
        D.code_operateur CLIENT_OPERATOR,
        D.operateur ROAMING_PARTNER_NAME, 
        (CASE
            WHEN trim(concat(A.location_mcc,"",A.location_mnc)) = '62402' THEN 'ROAMING SORTANT'
            ELSE 'ROAMING VISITEUR' 
        END) DIRECTION,
        trim(D.country_name) as ROAMING_PARTNER_COUNTRY
    FROM MON.SPARK_FT_MSC_TRANSACTION B
    INNER JOIN (select * from DIM.SPARK_DT_REF_OPERATEURS where cc in (235,236,237,240,241,243) )D 
    ON SUBSTR(B.other_party, 1, 3) = trim(D.cc) or SUBSTR(B.served_msisdn, 1, 3) = trim(D.cc)
    INNER JOIN 
    (SELECT * FROM
        (SELECT 
        Y.served_party,
        Y.other_party,
        Y.location_mcc,
        Y.location_mnc,
        Y.CHARGED_PARTY,
        Y.transaction_date EVENT_DATE
        FROM MON.SPARK_FT_BILLED_TRANSACTION_PREPAID Y WHERE Y.transaction_date = "2023-01-09" and  Y.location_mcc is not null AND Y.operator_code = 'OCM')L )A
    ON A.CHARGED_PARTY = B.other_party
    WHERE B.transaction_date = '2023-01-09' AND Subscriber_Type like "%ROAM%" AND B.transaction_type like "%TEL%" 
    group by 
        IMSI,TRANSACTION_DATE,DIRECTION,
        ROAMING_PARTNER_COUNTRY,
        CLIENT_OPERATOR,ROAMING_PARTNER_NAME,SERVICE_FAMILY,SERVED,OTHER ) R1

UNION

SELECT
distinct IMSI,
SERVED,
OTHER,
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
        concat(A.location_mcc,"",A.location_mnc,"",A.other_party)IMSI,
        B.other_party OTHER,
         A.CHARGED_PARTY SERVED,
        count(*) NUMBER_OF_CALLS,
        sum(nvl(B.transaction_duration,0))/60 MINUTES,
        B.transaction_type SERVICE_FAMILY,
        D.code_operateur CLIENT_OPERATOR,
        D.operateur ROAMING_PARTNER_NAME, 
        (CASE
            WHEN trim(concat(A.location_mcc,"",A.location_mnc)) = '62402' THEN 'ROAMING SORTANT'
            ELSE 'ROAMING VISITEUR' 
        END) DIRECTION,
        trim(D.country_name) as ROAMING_PARTNER_COUNTRY
    FROM MON.SPARK_FT_MSC_TRANSACTION B
    INNER JOIN (select * from DIM.SPARK_DT_REF_OPERATEURS where cc in (235,236,237,240,241,243) )D 
    ON SUBSTR(B.other_party, 1, 3) = trim(D.cc) or SUBSTR(B.served_msisdn, 1, 3) = trim(D.cc)
    INNER JOIN 
    (SELECT * FROM
        (SELECT 
        Y.served_party,
        Y.other_party,
        Y.location_mcc,
        Y.location_mnc,
        Y.CHARGED_PARTY,
        Y.transaction_date EVENT_DATE
        FROM MON.SPARK_FT_BILLED_TRANSACTION_POSTPAID Y WHERE Y.transaction_date = "2023-01-08" and  Y.location_mcc is not null AND Y.operator_code = 'OCM')L )A
    ON A.CHARGED_PARTY = B.other_party or B.served_msisdn = A.CHARGED_PARTY
    WHERE B.transaction_date = '2023-01-08' AND Subscriber_Type like "%ROAM%" AND B.transaction_type like "%TEL%" 
    group by 
        IMSI,TRANSACTION_DATE,DIRECTION,
        ROAMING_PARTNER_COUNTRY,
        CLIENT_OPERATOR,ROAMING_PARTNER_NAME,SERVICE_FAMILY,SERVED,OTHER ) R1



    LEFT JOIN 
    (SELECT * FROM
        (SELECT 
        Y.served_party,
        Y.other_party,
        Y.location_mcc,
        Y.location_mnc,
        Y.transaction_date EVENT_DATE
        FROM MON.SPARK_FT_BILLED_TRANSACTION_PREPAID Y WHERE Y.transaction_date = "2023-01-08" and  Y.location_mcc is not null)L )A
    ON A.served_party = B.served_msisdn 