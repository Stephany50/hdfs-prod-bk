INSERT INTO MON.SPARK_FT_ROAMING_CEMAC_IN_OUT PARTITION(CALL_DATE)

SELECT
CLIENT_OPERATOR,
DIRECTION,
ROAMING_PARTNER_NAME,
ROAMING_PARTNER_COUNTRY,
SERVICE_FAMILY,
NUMBER_OF_CALLS,
MINUTES,
IMSI,
CALL_DATE
FROM 
    (SELECT
    B.transaction_date CALL_DATE,
    B.served_imsi IMSI,
    count(*) NUMBER_OF_CALLS,
    sum(nvl(B.transaction_duration,0))/60 MINUTES,
    B.transaction_type SERVICES_FAMILY,
    'CMR02' CLIENT_OPERATOR,
    D.operateur ROAMING_PARTNER_NAME,
    'ROAMING SORTANT' DIRECTION,
    CASE 
      WHEN  substr(B.roaming_number,1,3)=235  THEN 'CHAD'
      WHEN  substr(B.roaming_number,1,3)=236 THEN ' CENTRAL AFRICAN REPUBLIC'
      WHEN  substr(B.roaming_number,1,3)=237 THEN 'CAMEROON'
      WHEN  substr(B.roaming_number,1,3)=240 THEN 'EQUATORIAL GUINEA'
      WHEN  substr(B.roaming_number,1,3)=241 THEN 'GABON'
      WHEN  substr(B.roaming_number,1,3)=243 THEN 'CONGO (DEMOCRATIC REPUBLIC)'
    END AS ROAMING_PARTNER_COUNTRY
    FROM MON.SPARK_FT_MSC_TRANSACTION B
    INNER JOIN (select * from DIM.SPARK_REF_OPERATEURS_CEMAC_NEW )D
    ON substr(B.roaming_number,1,length(trim(D.code_operateur))) = trim(D.code_operateur)
    WHERE  B.transaction_date = '###SLICE_VALUE###' and substr(B.served_imsi,1,5)="62402" and B.transaction_type like "%ROAM%" and substr(B.roaming_number,1,3) in (235,236,237,240,241,243)
    GROUP BY
    B.served_msisdn,B.other_party,B.served_imsi,B.transaction_date,
    D.operateur, B.transaction_type,B.roaming_number

    UNION

    SELECT
        B.transaction_date CALL_DATE,
        B.served_imsi IMSI,
        count(*) NUMBER_OF_CALLS,
        sum(nvl(B.transaction_duration,0))/60 MINUTES,
        B.transaction_type SERVICES_FAMILY,
        'CMR02' CLIENT_OPERATOR,
        D.operator_name ROAMING_PARTNER_NAME,
    'ROAMING VISITEUR' DIRECTION,
        trim(D.country_name) as ROAMING_PARTNER_COUNTRY
    FROM MON.SPARK_FT_MSC_TRANSACTION B
    INNER JOIN (select * from DIM.SPARK_REF_OPERATEURS_CEMAC )D
    ON substr(B.served_imsi,1,5) = trim(concat(D.mcc,"",D.mnc))
    WHERE B.transaction_date = '###SLICE_VALUE###' AND Subscriber_Type = "ROAM" AND B.transaction_type like "%TEL%"
    group by
        IMSI,TRANSACTION_DATE,DIRECTION,
        ROAMING_PARTNER_COUNTRY,
        CLIENT_OPERATOR,ROAMING_PARTNER_NAME,SERVICES_FAMILY) R1