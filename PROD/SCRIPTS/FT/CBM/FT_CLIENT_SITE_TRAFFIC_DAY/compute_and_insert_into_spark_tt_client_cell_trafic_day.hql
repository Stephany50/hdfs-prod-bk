
INSERT INTO TMP.spark_tt_client_cell_trafic_day
SELECT
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
       Fn_get_operator_code(served_msisdn) operator_code,
       '###SLICE_VALUE###'   EVENT_DATE
FROM   mon.spark_ft_msc_transaction a
WHERE  a.transaction_date = To_date ('###SLICE_VALUE###')
       AND a.served_party_location LIKE '624-02-%'
GROUP  BY served_msisdn,
          Substr(a.served_party_location, 14, 5),
          served_party_location,
          Fn_get_operator_code(served_msisdn)