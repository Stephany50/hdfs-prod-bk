 --add jar hdfs:///PROD/UDF/hive-udf-1.0.1.jar;

 --create temporary function FN_GET_OPERATOR_CODE as 'cm.orange.bigdata.udf.GetOperatorCode';

INSERT INTO mon.tt_client_cell_trafic_day
SELECT '###SLICE_VALUE###'   EVENT_DATE,
       served_msisdn  MSISDN,
       Substr(a.served_party_location, 14, 5)  AS LOCATION_CI,
       Sum (CASE
              WHEN a.transaction_direction = 'Sortant'
                   AND Substr (transaction_type, 1, 3) <> 'SMS' THEN
              transaction_duration
              ELSE 0
            END) DUREE_SORTANT,
       Sum (CASE
              WHEN a.transaction_direction = 'Sortant'
                   AND Substr (transaction_type, 1, 3) <> 'SMS' THEN 1
              ELSE 0
            END)  NBRE_TEL_SORTANT,
       Sum (CASE
              WHEN a.transaction_direction = 'Entrant'
                   AND Substr (transaction_type, 1, 3) <> 'SMS' THEN
              transaction_duration
              ELSE 0
            END)  DUREE_ENTRANT,
       Sum (CASE
              WHEN a.transaction_direction = 'Entrant'
                   AND Substr (transaction_type, 1, 3) <> 'SMS' THEN 1
              ELSE 0
            END)  NBRE_TEL_ENTRANT,
       Sum (CASE
              WHEN a.transaction_direction = 'Sortant'
                   AND Substr (transaction_type, 1, 3) = 'SMS' THEN 1
              ELSE 0
            END) NBRE_SMS_SORTANT,
       Sum (CASE
              WHEN a.transaction_direction = 'Entrant'
                   AND Substr (transaction_type, 1, 3) = 'SMS' THEN 1
              ELSE 0
            END)  NBRE_SMS_ENTRANT,
       served_party_location,
       Fn_get_operator_code(served_msisdn) operator_code
FROM   mon.ft_msc_transaction a
WHERE  a.transaction_date = To_date ('###SLICE_VALUE###')
       AND a.served_party_location LIKE '624-02-%'
GROUP  BY served_msisdn,
          Substr(a.served_party_location, 14, 5),
          served_party_location,
          Fn_get_operator_code(served_msisdn);