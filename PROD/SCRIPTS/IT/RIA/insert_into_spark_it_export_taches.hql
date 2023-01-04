INSERT INTO cdr.spark_it_export_taches 
SELECT
  id ,Type ,Status ,DueDate,
  SynchronizedAt,ReceivedAt,CompletedAt ,
  Latitude ,Longitude ,snd_created_by ,
  PoiId ,PoiLatitude ,PoiLongitude ,
  Secteur ,Zone_PMO ,Region_administrative ,
  Region_commerciale ,UserId ,Email ,
  SupervisorUsername ,SupervisorEmail ,
  TemplateId ,SurveyTemplateName ,
  CURRENT_TIMESTAMP() INSERT_DATE,
  ORIGINAL_FILE_NAME,
  ORIGINAL_FILE_SIZE,
  ORIGINAL_FILE_LINE_COUNT,
  TO_DATE(SUBSTRING (ORIGINAL_FILE_NAME, -14, 10)) ORIGINAL_FILE_DATE
FROM cdr.tt_mva C
LEFT JOIN (SELECT DISTINCT  ORIGINAL_FILE_NAME FILE_NAME FROM cdr.spark_it_export_taches WHERE ORIGINAL_FILE_DATE BETWEEN DATE_SUB(CURRENT_DATE,3) AND CURRENT_DATE )T ON T.FILE_NAME=C.ORIGINAL_FILE_NAME
WHERE T.FILE_NAME IS NULL
