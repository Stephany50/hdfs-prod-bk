create PROCEDURE     INSERT_DM_INFOS_ANNUAIRE
/*
    Desc : insertion des donnees "Infos annuaires" des MSISDN dans le datamart marketing
    Date : 25/11/2015 a 12:00
    Autheur : dimitri.happi@orange.cm
*/
    (
        s_slice_value IN VARCHAR2
    ) IS 
    s_result NUMBER;
    s_nbre_ligne NUMBER;
    d_begin_process DATE;
    nbre_attendu NUMBER;
BEGIN
    d_begin_process := SYSDATE;
    -- nombre de groupes de donnees attendu pour une journee
    nbre_attendu := 11;

    -- Pre-conditions : s'assurer que la table FT_MARKETING_DATAMART contient deja les infos obligatoires de cette journee
    SELECT
      ( CASE
            -- si pas encore traité 
            WHEN
                MON.FN_VALIDATE_DAY2DAY_EXIST ('mon.FT_MARKETING_DATAMART', 'EVENT_DATE'
                          , s_slice_value, s_slice_value, 10 ,  '') = 1
              AND
                MON.IS_BASE_IDENTIFICATION_OK(s_slice_value) = 1
              AND --// Verification dans la table de logging que les donnees terminal ne sont pas renseignes pour cette date
                NOT EXISTS(SELECT * FROM MON.LOGGING_DM_MKT_LOADING 
                            WHERE EVENT_DATE = TO_DATE(s_slice_value, 'yyyymmdd') AND DATA_GROUP = 'INFOS_ANNUAIRE')     
            THEN 0
            -- si déjà traité 
            ELSE  1
        END ) INTO s_result FROM DUAL;
            
    IF  s_result = 0 THEN
        
        MERGE INTO mon.FT_MARKETING_DATAMART a
        USING
            (
                SELECT MSISDN,
                    PRENOM AS DIR_FIRST_NAME,
                    NOM AS DIR_LAST_NAME,
                    NEE_LE AS DIR_BIRTH_DATE,
                    VILLE_VILLAGE AS DIR_IDENTIFICATION_TOWN,
                    TRUNC(DATE_IDENTIFICATION) AS DIR_IDENTIFICATION_DATE
                FROM DIM.DT_BASE_IDENTIFICATION
            ) b
                ON (a.MSISDN = b.MSISDN AND a.EVENT_DATE = TO_DATE(s_slice_value, 'yyyymmdd'))
         WHEN MATCHED THEN
            UPDATE SET a.DIR_FIRST_NAME = b.DIR_FIRST_NAME, a.DIR_LAST_NAME = b.DIR_LAST_NAME, a.DIR_BIRTH_DATE = b.DIR_BIRTH_DATE,
                       a.DIR_IDENTIFICATION_TOWN = b.DIR_IDENTIFICATION_TOWN, a.DIR_IDENTIFICATION_DATE = b.DIR_IDENTIFICATION_DATE,
                       a.REFRESH_DATE = SYSDATE
                   WHERE COALESCE(a.DIR_IDENTIFICATION_DATE,TO_DATE('17000101','yyyymmdd')) <> COALESCE(b.DIR_IDENTIFICATION_DATE,TO_DATE('17000101','yyyymmdd'));
                       
        COMMIT;
        
        /*******
        -- Logging du groupe de donnees LOCALISATION calculees pour cette date 
        *******/
            
        INSERT INTO LOGGING_DM_MKT_LOADING
        SELECT TO_DATE(s_slice_value, 'yyyymmdd') AS EVENT_DATE, 'INFOS_ANNUAIRE' AS DATA_GROUP, d_begin_process, SYSDATE,'DATAMART_MARKETING' AS FLUX_NAME FROM DUAL;
            
        COMMIT;
        
        SELECT COUNT(*) INTO s_nbre_ligne FROM MON.LOGGING_DM_MKT_LOADING 
                            WHERE EVENT_DATE = TO_DATE(s_slice_value, 'yyyymmdd')
                                AND FLUX_NAME = 'DATAMART_MARKETING';
        
        IF s_nbre_ligne = nbre_attendu THEN
            -- compression après insertion
            EXECUTE IMMEDIATE 'ALTER TABLE MON.FT_MARKETING_DATAMART MOVE PARTITION MARKETING_DATAMART_' || s_slice_value || ' TABLESPACE TAB_P_MON_J' || SUBSTR(s_slice_value, -2) || '_256M  PCTFREE 0 COMPRESS';
        END IF;
        
    END IF;
END;
/

