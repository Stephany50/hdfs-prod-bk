	CREATE OR REPLACE PROCEDURE MON.P_PROCESS_BASE_IDENTIFICATION
/*
    Desc : Traitement de la base des identification
    Date : 24/05/2016 a 18:27
    Autheur : dimitri.happi@orange.cm
*/
    (
        s_slice_value IN VARCHAR2
    ) IS 
    s_result INT;
    s_sql_query VARCHAR2(4000);
    d_table_maj DATE;
BEGIN
    d_table_maj := SYSDATE;
    -- Pre-conditions : 
    SELECT 
           CASE
                -- OK
                WHEN    
        MON.FN_VALIDATE_DAY2DAY_EXIST ('MON.DT_DATES_SCRIPT_PROCESSED',
                     'DATE_PROCESSED', s_slice_value, s_slice_value, 1,  ' AND SCRIPT_NAME = ''BASE_IDENTIFICATION''')
                    = 0
                    AND
            MON.FN_VALIDATE_DAY2DAY_EXIST ('CDR.IT_PREPAID_CLIENT_DIRECTORY', 'ORIGINAL_FILE_DATE',
                                TO_CHAR(TO_DATE ( s_slice_value, 'yyyymmdd') +1 , 'yyyymmdd'), 
                                TO_CHAR(TO_DATE ( s_slice_value, 'yyyymmdd')+1 , 'yyyymmdd'), 10, '') = 1
                    AND
         MON.FN_VALIDATE_DAY2DAY_EXIST ('CDR.IT_CLIENT_SNAPID_DIRECTORY', 'ORIGINAL_FILE_DATE',
                                                            s_slice_value,s_slice_value, 5, '')   = 1
                    AND
         MON.FN_VALIDATE_DAY2DAY_EXIST ('MON.TT_NOMAD_STATUT_DIRECTORY', 'ORIGINAL_FILE_DATE',
                                                            TO_CHAR(TO_DATE ( s_slice_value, 'yyyymmdd') +1 , 'yyyymmdd'),
                                                            TO_CHAR(TO_DATE ( s_slice_value, 'yyyymmdd') +1 , 'yyyymmdd'), 5, '')   = 1                                      
                THEN 1 
                -- NOK 
                ELSE  0
           END  INTO s_result
    FROM DUAL;    
             
    IF  s_result = 1 THEN
        -- Nettoyage table temporaire
        s_sql_query := 'TRUNCATE TABLE MON.TT_IDENTIFICATION_MSISDN';
        EXECUTE IMMEDIATE s_sql_query;
        COMMIT ;
        
                
        -- 1. Extraction des donnees d'identification les plus recentes pour chaque MSISDN
        -- Donnees base NSTOOL
        INSERT /*+ APPEND */ INTO TT_IDENTIFICATION_MSISDN
        SELECT MSISDN,
            NOM,
            PRENOM,
            NEE_LE,
            NEE_A,
            PROFESSION,
            QUARTIER_RESIDENCE,
            VILLE_VILLAGE,
            CNI,
            DATE_IDENTIFICATION,
            TYPE_DOCUMENT,
            FICHIER_CHARGEMENT,
            SYSDATE AS DATE_INSERTION,
            'NON' AS EST_SNAPPE,            
            IDENTIFICATEUR, 
            NULL AS GENRE, 
            NULL AS CIVILITE, 
            NULL AS TYPE_PIECE_IDENTIFICATION, 
            NULL AS PROFESSION_IDENTIFICATEUR
        FROM
        (
            SELECT 
                CASE WHEN MSISDN_NUM = 'MSISDN1' THEN MSISDN1 
                     WHEN MSISDN_NUM = 'MSISDN2' THEN MSISDN2
                     WHEN MSISDN_NUM = 'MSISDN3' THEN MSISDN3
                     ELSE MSISDN4 
                END AS MSISDN,
                NOM,
                PRENOM,
                NEE_LE,
                NEE_A,
                PROFESSION,
                QUARTIER_RESIDENCE,
                VILLE_VILLAGE,
                CNI,
                DATE_IDENTIFICATION,
                IDENTIFICATEUR,
                TYPE_DOCUMENT,
                FICHIER_CHARGEMENT,
                ROW_NUMBER() OVER (PARTITION BY CASE WHEN MSISDN_NUM = 'MSISDN1' THEN MSISDN1 
                                                 WHEN MSISDN_NUM = 'MSISDN2' THEN MSISDN2
                                                 WHEN MSISDN_NUM = 'MSISDN3' THEN MSISDN3
                                                 ELSE MSISDN4 
                                                END
                                   ORDER BY DATE_IDENTIFICATION DESC) AS RANG
            FROM
            (    
                SELECT 
                    MON.FN_FORMAT_MSISDN_TO_9DIGITS(CASE WHEN INSTR(NVL(NUMERO_TEL,''),' ',1,1) <> 0 THEN  SUBSTR(NVL(NUMERO_TEL,''), 1, INSTR(NVL(NUMERO_TEL,''),' ',1,1)-1) ELSE NUMERO_TEL END) AS MSISDN1,
                    MON.FN_FORMAT_MSISDN_TO_9DIGITS(CASE WHEN INSTR(NVL(NUMERO_TEL,''),' ',1,1) <> 0 THEN  SUBSTR(NVL(NUMERO_TEL,''), INSTR(NVL(NUMERO_TEL,''),' ',1,1)+1, DECODE(INSTR(NVL(NUMERO_TEL,''),' ',1,2), 0, LENGTH(NUMERO_TEL)+1, INSTR(NVL(NUMERO_TEL,''),' ',1,2))-INSTR(NVL(NUMERO_TEL,''),' ',1,1)-1) ELSE NULL END) AS MSISDN2,
                    MON.FN_FORMAT_MSISDN_TO_9DIGITS(CASE WHEN INSTR(NVL(NUMERO_TEL,''),' ',1,2) <> 0 THEN  SUBSTR(NVL(NUMERO_TEL,''), INSTR(NVL(NUMERO_TEL,''),' ',1,2)+1, DECODE(INSTR(NVL(NUMERO_TEL,''),' ',1,3), 0, LENGTH(NUMERO_TEL)+1, INSTR(NVL(NUMERO_TEL,''),' ',1,3))-INSTR(NVL(NUMERO_TEL,''),' ',1,2)-1) ELSE NULL END) AS MSISDN3,
                    MON.FN_FORMAT_MSISDN_TO_9DIGITS(CASE WHEN INSTR(NVL(NUMERO_TEL,''),' ',1,3) <> 0 THEN  SUBSTR(NVL(NUMERO_TEL,''), INSTR(NVL(NUMERO_TEL,''),' ',1,3)+1, DECODE(INSTR(NVL(NUMERO_TEL,''),' ',1,4), 0, LENGTH(NUMERO_TEL)+1, INSTR(NVL(NUMERO_TEL,''),' ',1,4))-INSTR(NVL(NUMERO_TEL,''),' ',1,3)-1) ELSE NULL END) AS MSISDN4,
                    NOM,
                    PRENOM,
                    COALESCE(MON.CONVERT_STR_TO_DATE(NEE_LE, 'yyyy-mm-dd hh24:mi:ss'),
                        MON.CONVERT_STR_TO_DATE(NEE_LE, 'dd/mm/RRRR')) AS NEE_LE,
                    NEE_A,
                    PROFESSION,
                    QUARTIER_RESIDENCE,
                    UPPER(VILLE_VILLAGE) AS VILLE_VILLAGE,
                    CNI,
                    TO_DATE(INDATE, 'yyyy-mm-dd hh24:mi:ss') AS DATE_IDENTIFICATION,
                    MON.FN_FORMAT_MSISDN_TO_9DIGITS(LTRIM(UTILISATEUR, '237')) AS IDENTIFICATEUR,
                    TYPE_DOCUMENT,
                    FICHIER_CHARGEMENT
                FROM CDR.IT_PREPAID_CLIENT_DIRECTORY 
                WHERE  ORIGINAL_FILE_DATE = TO_DATE (s_slice_value, 'yyyymmdd') + 1
            )
            CROSS JOIN
            (
                SELECT 'MSISDN1' AS MSISDN_NUM FROM DUAL UNION
                SELECT 'MSISDN2' AS MSISDN_NUM FROM DUAL UNION
                SELECT 'MSISDN3' AS MSISDN_NUM FROM DUAL UNION
                SELECT 'MSISDN4' AS MSISDN_NUM FROM DUAL
            )
            WHERE (CASE WHEN MSISDN_NUM = 'MSISDN1' THEN MSISDN1 
                     WHEN MSISDN_NUM = 'MSISDN2' THEN MSISDN2
                     WHEN MSISDN_NUM = 'MSISDN3' THEN MSISDN3
                     ELSE MSISDN4 
                END) IS NOT NULL
        )
        WHERE RANG = 1;
        
        COMMIT;
        
        -- Donnees base SNAP ID
        MERGE INTO TT_IDENTIFICATION_MSISDN a
        USING
        (
            SELECT MSISDN, NOM, PRENOM, NEE_LE,NEE_A, PROFESSION, QUARTIER_RESIDENCE, VILLE_VILLAGE,
                 CNI, DATE_IDENTIFICATION, TYPE_DOCUMENT, FICHIER_CHARGEMENT, DATE_INSERTION, EST_SNAPPE,
                 IDENTIFICATEUR, GENRE, CIVILITE, TYPE_PIECE_IDENTIFICATION, PROFESSION_IDENTIFICATEUR
            FROM
            (
                SELECT MSISDN, UPPER(NOM) AS NOM, UPPER(PRENOM) AS PRENOM, 
                    COALESCE(MON.CONVERT_STR_TO_DATE(DATENAISSANCE, 'dd/mm/yyyy hh24:mi:ss'),
                             MON.CONVERT_STR_TO_DATE(DATENAISSANCE, 'dd/mm/RRRR')) AS NEE_LE,
                     UPPER(LIEUNAISSANCE) AS NEE_A,
                     NULL AS PROFESSION,
                     UPPER(QUARTIER) AS QUARTIER_RESIDENCE,
                     UPPER(VILLE) AS VILLE_VILLAGE,
                     IDPIECEIDENTIFICATION AS CNI,
                     LASTMOD AS DATE_IDENTIFICATION,
                     NULL AS TYPE_DOCUMENT,
                     UPPER(SOURCE) AS FICHIER_CHARGEMENT,
                     SYSDATE AS DATE_INSERTION,
                     'NON' AS EST_SNAPPE,
                     LTRIM(SELLER_MSISDN, '237') AS IDENTIFICATEUR,
                     UPPER(GENRE) AS GENRE,
                     UPPER(CIVILITE) AS CIVILITE,
                     TYPEPIECEIDENTIFICATION AS TYPE_PIECE_IDENTIFICATION,
                     UPPER(PROFESSION) AS PROFESSION_IDENTIFICATEUR,
                     ROW_NUMBER() OVER (PARTITION BY MSISDN ORDER BY COALESCE(MON.CONVERT_STR_TO_DATE(DATEDERNIEREMODIF, 'dd/mm/yyyy hh24:mi:ss'),
                             MON.CONVERT_STR_TO_DATE(DATEDERNIEREMODIF, 'dd/mm/RRRR')) DESC) AS RG
                FROM CDR.IT_CLIENT_SNAPID_DIRECTORY
                WHERE ORIGINAL_FILE_DATE >= TO_DATE (s_slice_value, 'yyyymmdd') - 30 -- pour récuperer les plus d'identifications récentes
            )
            WHERE RG = 1 -- dedoublonnage    
        ) b
        ON (a.MSISDN = b.MSISDN)
        WHEN MATCHED THEN
            UPDATE SET
                a.NOM = b.NOM,
                a.PRENOM = b.PRENOM,
                a.NEE_LE = b.NEE_LE,
                a.NEE_A = b.NEE_A,
                a.QUARTIER_RESIDENCE = b.QUARTIER_RESIDENCE,
                a.VILLE_VILLAGE = b.VILLE_VILLAGE,
                a.CNI = b.CNI,
                a.DATE_IDENTIFICATION = b.DATE_IDENTIFICATION,
                a.FICHIER_CHARGEMENT = b.FICHIER_CHARGEMENT,
                a.IDENTIFICATEUR = b.IDENTIFICATEUR,
                a.GENRE = b.GENRE,
                a.CIVILITE = b.CIVILITE,
                a.TYPE_PIECE_IDENTIFICATION = b.TYPE_PIECE_IDENTIFICATION,
                a.PROFESSION_IDENTIFICATEUR = b.PROFESSION_IDENTIFICATEUR
        WHEN NOT MATCHED THEN
            INSERT (a.MSISDN, a.NOM, a.PRENOM, a.NEE_LE, a.NEE_A, a.PROFESSION, a.QUARTIER_RESIDENCE, a.VILLE_VILLAGE, a.CNI, 
                                 a.DATE_IDENTIFICATION, a.TYPE_DOCUMENT, a.FICHIER_CHARGEMENT, a.DATE_INSERTION, a.EST_SNAPPE, 
                                 a.IDENTIFICATEUR, a.GENRE, a.CIVILITE, a.TYPE_PIECE_IDENTIFICATION, a.PROFESSION_IDENTIFICATEUR)
                    VALUES (b.MSISDN, b.NOM, b.PRENOM, b.NEE_LE, b.NEE_A, b.PROFESSION, b.QUARTIER_RESIDENCE, b.VILLE_VILLAGE, b.CNI, 
                                 b.DATE_IDENTIFICATION, b.TYPE_DOCUMENT, b.FICHIER_CHARGEMENT, SYSDATE, b.EST_SNAPPE, 
                                 b.IDENTIFICATEUR, b.GENRE, b.CIVILITE, b.TYPE_PIECE_IDENTIFICATION, b.PROFESSION_IDENTIFICATEUR);
        
        COMMIT;
        
        -- 2. Mise à jour des identifications extantes et ajout des nouvelles pour les MSISDN Correctes issues de SNAPID                 
        MERGE INTO DIM.DT_BASE_IDENTIFICATION a
        USING
        (
            SELECT MSISDN, NOM, PRENOM, NEE_LE, NEE_A, PROFESSION, QUARTIER_RESIDENCE, 
                VILLE_VILLAGE, CNI, DATE_IDENTIFICATION, TYPE_DOCUMENT, FICHIER_CHARGEMENT, EST_SNAPPE, IDENTIFICATEUR
                , GENRE, CIVILITE, TYPE_PIECE_IDENTIFICATION, PROFESSION_IDENTIFICATEUR
            FROM TT_IDENTIFICATION_MSISDN
            WHERE FN_GET_OPERATOR_CODE(MSISDN)  IN ('OCM', 'SET') AND IS_NUMBER(MSISDN) = 'Y'
        ) b
        ON (a.MSISDN = b.MSISDN)
        WHEN MATCHED THEN
            UPDATE 
            SET a.CNI = b.CNI,
                a.NOM = b.NOM,
                a.PRENOM = b.PRENOM,
                a.NEE_LE = b.NEE_LE,
                a.NEE_A = b.NEE_A,
                a.PROFESSION = b.PROFESSION,
                a.QUARTIER_RESIDENCE = b.QUARTIER_RESIDENCE,
                a.VILLE_VILLAGE = b.VILLE_VILLAGE,
                a.DATE_IDENTIFICATION = b.DATE_IDENTIFICATION,
                a.TYPE_DOCUMENT = b.TYPE_DOCUMENT,
                a.FICHIER_CHARGEMENT = b.FICHIER_CHARGEMENT,
                a.EST_SNAPPE = b.EST_SNAPPE,
                a.IDENTIFICATEUR = b.IDENTIFICATEUR,
                a.DATE_MISE_A_JOUR = SYSDATE,
                a.DATE_TABLE_MIS_A_JOUR = d_table_maj,
                a.GENRE = b.GENRE,
                a.CIVILITE = b.CIVILITE,
                a.TYPE_PIECE_IDENTIFICATION = b.TYPE_PIECE_IDENTIFICATION,
                a.PROFESSION_IDENTIFICATEUR = b.PROFESSION_IDENTIFICATEUR
            WHERE a.DATE_IDENTIFICATION < b.DATE_IDENTIFICATION
        WHEN NOT MATCHED THEN
            INSERT (a.MSISDN, a.NOM, a.PRENOM, a.NEE_LE, a.NEE_A, a.PROFESSION, a.QUARTIER_RESIDENCE, a.VILLE_VILLAGE, a.CNI, 
                         a.DATE_IDENTIFICATION, a.TYPE_DOCUMENT, a.FICHIER_CHARGEMENT, a.DATE_INSERTION, a.EST_SNAPPE, a.IDENTIFICATEUR, 
                         a.DATE_MISE_A_JOUR, a.DATE_TABLE_MIS_A_JOUR, a.GENRE, a.CIVILITE, a.TYPE_PIECE_IDENTIFICATION, a.PROFESSION_IDENTIFICATEUR)
            VALUES (b.MSISDN, b.NOM, b.PRENOM, b.NEE_LE, b.NEE_A, b.PROFESSION, b.QUARTIER_RESIDENCE, b.VILLE_VILLAGE, b.CNI, 
                         b.DATE_IDENTIFICATION, b.TYPE_DOCUMENT, b.FICHIER_CHARGEMENT, SYSDATE, b.EST_SNAPPE, b.IDENTIFICATEUR, SYSDATE, d_table_maj
                         , b.GENRE, b.CIVILITE, b.TYPE_PIECE_IDENTIFICATION, b.PROFESSION_IDENTIFICATEUR);
          commit;
                         
      --Mis à jour de la table dim.dt_base_identification à partir des données issues de NOMAD
      merge into dim.dt_base_identification a
      using(
         select TELEPHONE,NOMDUCLIENT,PRENOMDUCLIENT,DATEDENAISSANCE,LIEUDENAISSANCE,PROFESSION,QUARTIER,VILLE,NUMEROPIECE,
        DATE_IDENTIFICATION,TYPE_DOCUMENT,fichier_chargement,DATE_INSERTION,EST_SNAPPE,ETAT, ETATDEXPORTGLOBAL,IDENTIFICATEUR,
        date_mise_a_jour,date_table_mis_a_jour,genre,civilite,TYPE_PIECE_IDENTIFICATION,PROFESSION_IDENTIFICATEUR,MOTIF_REJET
        from 
        (
        select  TELEPHONE ,
                    NOMDUCLIENT,
                    PRENOMDUCLIENT,
                    to_date(DATEDENAISSANCE,'yyyy-mm-dd') DATEDENAISSANCE,
                    LIEUDENAISSANCE,
                    null PROFESSION,
                    QUARTIER,
                    VILLE,
                    NUMEROPIECE,
                     to_date(substr(EMISLE,1,10),'yyyy-mm-dd')  DATE_IDENTIFICATION,
                    null TYPE_DOCUMENT,
                    'NOMAD' fichier_chargement,
                   to_date(substr(MAJLE,1,10),'yyyy-mm-dd') DATE_INSERTION,
                   -- (Case when ETAT='VALID' then '1' else null end) EST_SNAPPE,
                   (CASE WHEN upper(ETAT)='VALID' and upper(ETATDEXPORTGLOBAL)='SUCCESS' then 'OUI'
                          WHEN upper(ETAT)='INVALID' then 'NON'  else 'UNKNOWN' END )EST_SNAPPE,
                    ETAT ,
                    ETATDEXPORTGLOBAL,
                    LOGINVENDEUR IDENTIFICATEUR,
                    to_date(substr(MAJLE,1,10),'yyyy-mm-dd')  date_mise_a_jour,
                    to_date(substr(MAJLE,1,10),'yyyy-mm-dd')  date_table_mis_a_jour,
                    (Case when TITRE ='Madame(Mme)' then 'F' else 'M' END) genre,
                    TITRE  civilite,
                     piece TYPE_PIECE_IDENTIFICATION,
                    null PROFESSION_IDENTIFICATEUR,
                     null MOTIF_REJET ,
                     ROW_NUMBER() OVER (PARTITION BY TELEPHONE ORDER BY to_date(substr(MAJLE,1,10),'yyyy-mm-dd') DESC) AS RG
                    from MON.TT_NOMAD_STATUT_DIRECTORY
                    where original_file_date = TO_DATE (s_slice_value, 'yyyymmdd') + 1 and TYPEDECONTRAT='Nouvel Abonnement' and  ETATDEXPORTGLOBAL ='SUCCESS' and LOGINVENDEUR not in ('testfo','NKOLBONG','testve')    
                 )   where RG =1
            ) b
         on (a.MSISDN = b.TELEPHONE)
 WHEN MATCHED THEN
    UPDATE SET  
            a.NOM  =b.NOMDUCLIENT,
            a.PRENOM=b.PRENOMDUCLIENT,
            a.NEE_LE=b.DATEDENAISSANCE,
            a.NEE_A =b.LIEUDENAISSANCE,
            a.PROFESSION=b.PROFESSION,
            a.QUARTIER_RESIDENCE=b.QUARTIER,
            a.VILLE_VILLAGE =b.VILLE,
            a.CNI=b.NUMEROPIECE,
            a.DATE_IDENTIFICATION=b.DATE_IDENTIFICATION,
            a.TYPE_DOCUMENT=b.TYPE_DOCUMENT,
            a.FICHIER_CHARGEMENT=b.fichier_chargement,
            a.DATE_INSERTION =b.DATE_INSERTION,
            a.EST_SNAPPE =b. EST_SNAPPE,
            a.IDENTIFICATEUR =b. IDENTIFICATEUR,
            a.DATE_MISE_A_JOUR =b.date_mise_a_jour,
            a.DATE_TABLE_MIS_A_JOUR =b.date_table_mis_a_jour,
            a.GENRE=b.genre,
            a.CIVILITE =b.CIVILITE,
            a.TYPE_PIECE_IDENTIFICATION =b.TYPE_PIECE_IDENTIFICATION,
            a.PROFESSION_IDENTIFICATEUR =b.PROFESSION_IDENTIFICATEUR,
            a.MOTIF_REJET=b.MOTIF_REJET
         WHEN NOT MATCHED THEN
         INSERT(a.MSISDN,a.NOM,a.PRENOM,a.NEE_LE,a.NEE_A,a.PROFESSION,a.QUARTIER_RESIDENCE,a.VILLE_VILLAGE,a.CNI,a.DATE_IDENTIFICATION    ,
                a.TYPE_DOCUMENT,a.FICHIER_CHARGEMENT,a.DATE_INSERTION,a.EST_SNAPPE,a.IDENTIFICATEUR,a.DATE_MISE_A_JOUR,a.DATE_TABLE_MIS_A_JOUR   ,
                 a.GENRE,a.CIVILITE,a.TYPE_PIECE_IDENTIFICATION,a.PROFESSION_IDENTIFICATEUR,a.MOTIF_REJET)
           VALUES (b.TELEPHONE,b.NOMDUCLIENT,b.PRENOMDUCLIENT,b.DATEDENAISSANCE,b.LIEUDENAISSANCE,b.PROFESSION,b.QUARTIER,b.VILLE,b.NUMEROPIECE,
                   b.DATE_IDENTIFICATION,b.TYPE_DOCUMENT,b.fichier_chargement,b.DATE_INSERTION,b. EST_SNAPPE,b. IDENTIFICATEUR,b.date_mise_a_jour,b.date_table_mis_a_jour,b.genre,
                   b.CIVILITE ,b.TYPE_PIECE_IDENTIFICATION,b.PROFESSION_IDENTIFICATEUR,b.MOTIF_REJET);
           
          commit;
      
            
        /*******  
        -- Logging des donnees calculees pour cette date 
        *******/
                 
        INSERT INTO DT_DATES_SCRIPT_PROCESSED
        VALUES (TO_DATE(s_slice_value, 'yyyymmdd'), 'BASE_IDENTIFICATION', SYSDATE);
        
        -- validation des mises a jour et insertion
        COMMIT;
        
        UPDATE DIM.DT_BASE_IDENTIFICATION
        SET DATE_TABLE_MIS_A_JOUR = d_table_maj;
        
        COMMIT;
        
        -- 3. Sauvegarde des identifications dont les MSISDN sont Incorrectes dans la table TT_BAD_IDENTIFICATION_MSISDN
        MERGE INTO MON.TT_BAD_IDENTIFICATION_MSISDN a
        USING
        (
            SELECT MSISDN, NOM, PRENOM, NEE_LE, NEE_A, PROFESSION, QUARTIER_RESIDENCE, 
                VILLE_VILLAGE, CNI, DATE_IDENTIFICATION, TYPE_DOCUMENT, FICHIER_CHARGEMENT, EST_SNAPPE, IDENTIFICATEUR
                , GENRE, CIVILITE, TYPE_PIECE_IDENTIFICATION, PROFESSION_IDENTIFICATEUR
            FROM TT_IDENTIFICATION_MSISDN
            WHERE FN_GET_OPERATOR_CODE(MSISDN)  NOT IN ('OCM', 'SET') OR IS_NUMBER(MSISDN) = 'N'
        ) b
        ON (a.MSISDN = b.MSISDN)
        WHEN MATCHED THEN
            UPDATE 
            SET a.CNI = b.CNI,
                a.NOM = b.NOM,
                a.PRENOM = b.PRENOM,
                a.NEE_LE = b.NEE_LE,
                a.NEE_A = b.NEE_A,
                a.PROFESSION = b.PROFESSION,
                a.QUARTIER_RESIDENCE = b.QUARTIER_RESIDENCE,
                a.VILLE_VILLAGE = b.VILLE_VILLAGE,
                a.DATE_IDENTIFICATION = b.DATE_IDENTIFICATION,
                a.TYPE_DOCUMENT = b.TYPE_DOCUMENT,
                a.FICHIER_CHARGEMENT = b.FICHIER_CHARGEMENT,
                a.EST_SNAPPE = b.EST_SNAPPE,
                a.IDENTIFICATEUR = b.IDENTIFICATEUR,
                a.DATE_MISE_A_JOUR = SYSDATE,
                a.DATE_TABLE_MIS_A_JOUR = d_table_maj,
                a.GENRE = b.GENRE,
                a.CIVILITE = b.CIVILITE,
                a.TYPE_PIECE_IDENTIFICATION = b.TYPE_PIECE_IDENTIFICATION,
                a.PROFESSION_IDENTIFICATEUR = b.PROFESSION_IDENTIFICATEUR
            WHERE a.DATE_IDENTIFICATION < b.DATE_IDENTIFICATION
        WHEN NOT MATCHED THEN
            INSERT (a.MSISDN, a.NOM, a.PRENOM, a.NEE_LE, a.NEE_A, a.PROFESSION, a.QUARTIER_RESIDENCE, a.VILLE_VILLAGE, a.CNI, 
                         a.DATE_IDENTIFICATION, a.TYPE_DOCUMENT, a.FICHIER_CHARGEMENT, a.DATE_INSERTION, a.EST_SNAPPE, a.IDENTIFICATEUR, 
                         a.DATE_MISE_A_JOUR, a.DATE_TABLE_MIS_A_JOUR, a.GENRE, a.CIVILITE, a.TYPE_PIECE_IDENTIFICATION, a.PROFESSION_IDENTIFICATEUR)
            VALUES (b.MSISDN, b.NOM, b.PRENOM, b.NEE_LE, b.NEE_A, b.PROFESSION, b.QUARTIER_RESIDENCE, b.VILLE_VILLAGE, b.CNI, 
                         b.DATE_IDENTIFICATION, b.TYPE_DOCUMENT, b.FICHIER_CHARGEMENT, SYSDATE, b.EST_SNAPPE, b.IDENTIFICATEUR, SYSDATE, d_table_maj
                         , b.GENRE, b.CIVILITE, b.TYPE_PIECE_IDENTIFICATION, b.PROFESSION_IDENTIFICATEUR);
                         
        COMMIT;
        
        UPDATE MON.TT_BAD_IDENTIFICATION_MSISDN
        SET DATE_TABLE_MIS_A_JOUR = d_table_maj;
        
        COMMIT;
        
        -- Nettoyage table temporaire
--        s_sql_query := 'TRUNCATE TABLE MON.TT_IDENTIFICATION_MSISDN';
--        EXECUTE IMMEDIATE s_sql_query;
--        COMMIT ;
    END IF;
END;
/