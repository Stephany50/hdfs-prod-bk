create PROCEDURE     INSERT_DM_LOCALISATION
/*
    Desc : insertion des donnees "Localisation" des MSISDN dans le datamart marketing
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
                MON.FN_VALIDATE_DAY2DAY_EXIST ('mon.FT_CLIENT_LAST_SITE_DAY', 'EVENT_DATE'
                          , s_slice_value, s_slice_value, 10 ,  '') = 1
              AND --// Verification dans la table de logging que les donnees terminal ne sont pas renseignes pour cette date
                NOT EXISTS(SELECT * FROM MON.LOGGING_DM_MKT_LOADING 
                            WHERE EVENT_DATE = TO_DATE(s_slice_value, 'yyyymmdd') AND DATA_GROUP = 'LOCALISATION')     
            THEN 0
            -- si déjà traité 
            ELSE  1
        END ) INTO s_result FROM DUAL;
            
    IF  s_result = 0 THEN
        
        MERGE INTO mon.FT_MARKETING_DATAMART a
        USING
            (
                SELECT MSISDN, 
                    max(SITE_NAME) AS LOC_SITE_NAME,
                    max(TOWNNAME) AS LOC_TOWN_NAME,
                    max(ADMINISTRATIVE_REGION) AS LOC_ADMINTRATIVE_REGION,
                    max(COMMERCIAL_REGION) AS LOC_COMMERCIAL_REGION 
                FROM FT_CLIENT_LAST_SITE_DAY                    
                    WHERE EVENT_DATE = TO_DATE(s_slice_value, 'yyyymmdd')
                        AND FN_GET_OPERATOR_CODE(MSISDN) IN ('SET', 'OCM')
                group by msisdn
            ) b
                ON (a.MSISDN = b.MSISDN AND a.EVENT_DATE = TO_DATE(s_slice_value, 'yyyymmdd'))
         WHEN MATCHED THEN
            UPDATE SET a.LOC_SITE_NAME = b.LOC_SITE_NAME, a.LOC_TOWN_NAME = b.LOC_TOWN_NAME, a.LOC_ADMINTRATIVE_REGION = b.LOC_ADMINTRATIVE_REGION,
                       a.LOC_COMMERCIAL_REGION = b.LOC_COMMERCIAL_REGION, a.REFRESH_DATE = SYSDATE
                    WHERE COALESCE(a.LOC_SITE_NAME,'UNKNOWN') <> COALESCE(b.LOC_SITE_NAME,'UNKNOWN');
                       
        COMMIT;
        
        /*******
        -- Logging du groupe de donnees LOCALISATION calculees pour cette date 
        *******/
            
        INSERT INTO LOGGING_DM_MKT_LOADING
        SELECT TO_DATE(s_slice_value, 'yyyymmdd') AS EVENT_DATE, 'LOCALISATION' AS DATA_GROUP, d_begin_process, SYSDATE,'DATAMART_MARKETING' AS FLUX_NAME FROM DUAL;
            
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

