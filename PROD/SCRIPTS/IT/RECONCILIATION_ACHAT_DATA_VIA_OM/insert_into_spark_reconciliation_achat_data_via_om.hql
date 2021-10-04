INSERT INTO CDR.SPARK_IT_RECONCILIATION_ACHAT_DATA_VIA_OM
SELECT
  ORIGINAL_FILE_NAME,
  ORIGINAL_FILE_SIZE,
  ORIGINAL_FILE_LINE_COUNT,
  IDTXN,
  MSISDN_PAYER,
  MSISDN_PAYEE,
  IPP_CODE,
  BUNDLE_NAME,
  BUNDLE_PRICE,
  BUNDLE_DATA_QTY,
  STATUS_TANGO_MP,
  STATUS_IN_REQUEST,
  TIMESTAMP_TXN  TIMESTAMP,
  ID_REQUEST_IN,
  CURRENT_TIMESTAMP() INSERT_DATE,
  TO_DATE(TIMESTAMP_TXN) TRANSACTION_DATE
FROM CDR.TT_RECONCILIATION_ACHAT_DATA_VIA_OM C
LEFT JOIN (SELECT DISTINCT  ORIGINAL_FILE_NAME FILE_NAME FROM CDR.SPARK_IT_RECONCILIATION_ACHAT_DATA_VIA_OM WHERE TRANSACTION_DATE BETWEEN DATE_SUB(CURRENT_DATE,7) AND CURRENT_DATE() )T ON T.FILE_NAME=C.ORIGINAL_FILE_NAME
WHERE T.FILE_NAME IS NULL

