INSERT INTO CDR.SPARK_IT_CX_USSD PARTITION (ORIGINAL_FILE_DATE)
SELECT
  SessionId ,
  StartDate ,
  FirstResponseDate ,
  EndDate ,
  surveyState ,
  ussd ,
  precision_1 ,
  transaction_realise ,
  mesure_satisfaction ,
  rubrique_organise ,
  information_claire ,
  information_trouvee ,
  utilite ,
  facilite_information ,
  perception ,
  axe_amelioration ,
  authorisation_rappel ,
  msisdn ,
  CURRENT_TIMESTAMP() INSERT_DATE,
  ORIGINAL_FILE_NAME,
  ORIGINAL_FILE_SIZE,
  ORIGINAL_FILE_LINE_COUNT,
  TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(SUBSTRING (ORIGINAL_FILE_NAME, -18, 8),'yyyyMMdd'))) ORIGINAL_FILE_DATE
FROM CDR.TT_CX_USSD C
LEFT JOIN (SELECT DISTINCT  ORIGINAL_FILE_NAME FILE_NAME FROM CDR.SPARK_IT_CX_USSD WHERE ORIGINAL_FILE_DATE BETWEEN DATE_SUB(CURRENT_DATE,3) AND CURRENT_DATE )T ON T.FILE_NAME=C.ORIGINAL_FILE_NAME
WHERE T.FILE_NAME IS NULL
