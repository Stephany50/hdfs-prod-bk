INSERT INTO CDR.SPARK_IT_CRM_MANDATAIRE_BASE
SELECT
    new_mandatairepartenaireId, 
    new_NomPrenom, 
    new_Numerodepicedumandataire, 
    new_NumerodecompteOMdumandataire, 
    new_NumerodelIBUdumandataire,
    new_Partenaire, 
    new_Datedenaissance, 
    new_Nationalite,
    new_Typepieceidentite, 
    new_Statutdumandataire, 
    new_Nom, 
    new_Prenom, 
    new_Sexe, 
    new_Identifiantinternedumandataire, 
    new_Qualitdumandataire, 
    new_DatedExpirationdelapiece, 
    new_DelivrerA,
    original_file_name,
    original_file_size,
    original_file_line_count,
    CURRENT_TIMESTAMP() INSERT_DATE,
    TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(SUBSTRING (original_file_name, 12, 8),'yyyyMMdd'))) original_file_date
FROM   CDR.TT_CRM_MANDATAIRE_BASE C
       LEFT JOIN (SELECT DISTINCT original_file_name FILE_NAME FROM   CDR.SPARK_IT_CRM_MANDATAIRE_BASE) T ON T.file_name = C.original_file_name
WHERE  T.file_name IS NULL