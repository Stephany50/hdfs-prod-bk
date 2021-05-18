INSERT INTO CDR.SPARK_IT_AGING_BALANCE
SELECT
code_client,
account_number,
account_name,
categ,
payment_term,
statut,
nom,
balance,
to_date(date_derniere_facture) date_derniere_facture,
au_zero_jrs,
au_moins_trente_jrs,
au_moins_soixante_jrs,
au_moins_quatre_vingt_dix_jrs,
au_moins_cent_vingt_jrs,
au_moins_cent_cinquante_jrs,
au_moins_cent_quatre_vingt_jrs,
au_moins_trois_cent_soixante_jrs,
au_moins_sept_cent_vingt_jrs,
sup_sept_vent_vingt_jrs,
trim(ORIGINAL_FILE_NAME) AS ORIGINAL_FILE_NAME,
current_timestamp() AS INSERT_DATE,
TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(SUBSTRING(ORIGINAL_FILE_NAME, -19, 8),'yyyyMMdd'))) AS_OF_DATE
FROM CDR.SPARK_TT_AGING_BALANCE C
  LEFT JOIN (SELECT DISTINCT ORIGINAL_FILE_NAME FILE_NAME FROM CDR.SPARK_IT_AGING_BALANCE) T ON T.FILE_NAME = C.ORIGINAL_FILE_NAME
WHERE  T.FILE_NAME IS NULL


