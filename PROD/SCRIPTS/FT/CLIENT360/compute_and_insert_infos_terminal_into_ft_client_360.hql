create PROCEDURE     INSERT_DM_TERMINAL_INFO
/*
    Desc : insertion des donnees "infos terminal" des MSISDN dans le datamart marketing
    Date : 23/11/2015 a 09:38
    Autheur : dimitri.happi@orange.cm
    UPDATE : ajout de l'imei dans la table ft_marketing_datamart par ronny.samo@orange.com le 24/08/2018
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
                MON.FN_VALIDATE_MONTH2MONTH_EXIST ('MON.FT_IMEI_TRAFFIC_MONTHLY', 'SMONTH', TO_CHAR(ADD_MONTHS(TO_DATE(s_slice_value, 'yyyymmdd'), -1), 'yyyymm'), 
                    TO_CHAR(ADD_MONTHS(TO_DATE(s_slice_value, 'yyyymmdd'), -1), 'yyyymm'), 10 ,  '') = 1
              AND --// Verification dans la table de logging que les donnees terminal ne sont pas renseignes pour cette date
                NOT EXISTS(SELECT * FROM MON.LOGGING_DM_MKT_LOADING 
                            WHERE EVENT_DATE = TO_DATE(s_slice_value, 'yyyymmdd') AND DATA_GROUP = 'INFOS_TERMINAL')     
            THEN 0
            -- si déjà traité 
            ELSE  1
        END ) INTO s_result FROM DUAL;
            

    IF  s_result = 0 THEN
        
        MERGE INTO mon.FT_MARKETING_DATAMART a
        USING
            (
                  SELECT MSISDN, SUBSTR(IMEI_MOST_USED,1,8) AS TER_TAC_CODE, SUBSTR(IMEI_MOST_USED,1,14) TER_IMEI, CONSTRUCTOR AS TER_CONSTRUCTOR, MODEL AS TER_MODEL, 
                      TECHNOLOGIE AS TER_2G_3G_4G_COMPATIBILITY
                      , (CASE WHEN TECHNOLOGIE = '2G' THEN 'O' ELSE 'N' END) AS TER_2G_COMPATIBILITY
                      , (CASE WHEN TECHNOLOGIE IN ('2.5G', '2.75G', '3G') THEN 'O' ELSE 'N' END) AS TER_3G_COMPATIBILITY
                      , (CASE WHEN TECHNOLOGIE = '4G' THEN 'O' ELSE 'N' END) AS TER_4G_COMPATIBILITY
                  FROM
                  --supression d'une requete imbriquee  (ajout suite au pb de calcul a cause de l'espace sur le TBS TMP)
                      (
                         SELECT DISTINCT MSISDN
                             , FIRST_VALUE(IMEI) OVER (PARTITION BY MSISDN ORDER BY TOTAL_DAYS_COUNT DESC) IMEI_MOST_USED
                         FROM MON.FT_IMEI_TRAFFIC_MONTHLY
                         WHERE SMONTH = TO_CHAR(ADD_MONTHS(TO_DATE(s_slice_value, 'yyyymmdd'), -1), 'yyyymm')                        
                       )
                  , DIM.DT_HANDSET_REF
                  WHERE  SUBSTR(IMEI_MOST_USED,1,8) = TAC_CODE --and ROWNUM = 1
            ) b
                ON (a.MSISDN = b.MSISDN AND a.EVENT_DATE = TO_DATE(s_slice_value, 'yyyymmdd'))
         WHEN MATCHED THEN
            UPDATE SET a.TER_TAC_CODE = b.TER_TAC_CODE, a.TER_IMEI= b.TER_IMEI, a.TER_CONSTRUCTOR = b.TER_CONSTRUCTOR, a.TER_MODEL = b.TER_MODEL,
                       a.TER_2G_3G_4G_COMPATIBILITY = b.TER_2G_3G_4G_COMPATIBILITY, a.TER_2G_COMPATIBILITY = b.TER_2G_COMPATIBILITY,
                       a.TER_3G_COMPATIBILITY = b.TER_3G_COMPATIBILITY, a.TER_4G_COMPATIBILITY = b.TER_4G_COMPATIBILITY, a.REFRESH_DATE = SYSDATE
            WHERE NVL(a.TER_TAC_CODE,'UNKNOWN') <> NVL(b.TER_TAC_CODE,'UNKNOWN')
                    AND NVL(a.TER_IMEI,'UNKNOWN') <> NVL(b.TER_IMEI,'UNKNOWN');  --limiter la maj aux terminaux qui ont changes (ajout suite au pb de calcul a cause de l'espace sur le TBS TMP)
                COMMIT;
        
        /*******
        -- Logging du groupe de donnees INFOS_TERMINAL calculees pour cette date 
        *******/
            
        INSERT INTO LOGGING_DM_MKT_LOADING
        SELECT TO_DATE(s_slice_value, 'yyyymmdd') AS EVENT_DATE, 'INFOS_TERMINAL' AS DATA_GROUP, d_begin_process, SYSDATE,'DATAMART_MARKETING' AS FLUX_NAME FROM DUAL;
            
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

