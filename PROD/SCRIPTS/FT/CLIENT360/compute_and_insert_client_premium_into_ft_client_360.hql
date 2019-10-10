create PROCEDURE     INSERT_DM_CLIENT_PREMIUM
/*
    Desc : insertion des donnees " Client prenium et prenium plus" des MSISDN dans le datamart marketing
    Date : 21/03/2017 a 15:50
    Autheur : alex.cheuko@orange.cm
*/
    (
        s_slice_value IN VARCHAR2 -- au format yyyymmdd
    ) IS 
    s_result NUMBER;
    s_nbre_ligne NUMBER;
    d_begin_process DATE;
    nbre_attendu NUMBER;
    d_slice_value DATE;
    s_month VARCHAR2(10);
BEGIN
    d_begin_process := SYSDATE;
    -- nombre de groupes de donnees attendu pour une journee
    nbre_attendu := 11;
    
    d_slice_value := TO_DATE(s_slice_value, 'yyyymmdd');
    s_month := TO_CHAR(d_slice_value, 'yyyymm');
    
    -- Pre-conditions : s'assurer que la table FT_MARKETING_DATAMART contient deja les infos obligatoires de cette journee
    SELECT
      ( CASE
            -- si pas encore traité 
            WHEN
                MON.FN_VALIDATE_DAY2DAY_EXIST ('mon.FT_MARKETING_DATAMART', 'EVENT_DATE'
                          , s_slice_value, s_slice_value, 10 ,  '') = 1
              AND
                -- verifier que les donnes du mois en cours sont dispo
                MON.FN_VALIDATE_MONTH2MONTH_EXIST ('MON.FT_MSISDN_PREMIUM_MONTH', 'EVENT_MONTH'
                                                            , s_month,  s_month, 10, '') = 1
              AND --// Verification dans la table de logging que les donnees premimus ne sont pas renseignes pour cette date
                NOT EXISTS(SELECT * FROM MON.LOGGING_DM_MKT_LOADING 
                            WHERE EVENT_DATE = TO_DATE(s_slice_value, 'yyyymmdd') AND DATA_GROUP = 'CLIENT_PREMIUM')  
            THEN 0
            -- si déjà traité 
            ELSE  1
      END ) INTO s_result FROM DUAL;
            
    IF  s_result = 0 THEN
        
        MERGE INTO MON.FT_MARKETING_DATAMART a
        USING
            (
                SELECT DISTINCT
                    MSISDN,
                    REVENU_MOYEN,
                    PREMIUM,
                    CONSO_MOY_DATA,
                    RECHARGE_MOY,
                    PREMIUM_PLUS
                FROM MON.FT_MSISDN_PREMIUM_MONTH
                WHERE EVENT_MONTH = s_month
            ) b
                ON (a.MSISDN = b.MSISDN AND  a.EVENT_DATE = d_slice_value)
         WHEN MATCHED THEN
            UPDATE SET a.REVENU_MOYEN = b.REVENU_MOYEN, a.PREMIUM = b.PREMIUM, a.CONSO_MOY_DATA = b.CONSO_MOY_DATA,
                       a.RECHARGE_MOY = b.RECHARGE_MOY, a.PREMIUM_PLUS = b.PREMIUM_PLUS
                      ,a.REFRESH_DATE = SYSDATE
                   WHERE COALESCE(a.REVENU_MOYEN,-1) <> COALESCE(b.REVENU_MOYEN,-1)
                        OR COALESCE(a.PREMIUM,-1) <> COALESCE(b.PREMIUM,-1)
                        OR COALESCE(a.CONSO_MOY_DATA,-1) <> COALESCE(b.CONSO_MOY_DATA,-1)
                        OR COALESCE(a.RECHARGE_MOY,-1) <> COALESCE(b.RECHARGE_MOY,-1)
                        OR COALESCE(a.PREMIUM_PLUS,-1) <> COALESCE(b.PREMIUM_PLUS,-1);
        
        /*******
        -- Logging du groupe de donnees CLIENT_PRENIUM calculees pour cette date 
        *******/
            
        INSERT INTO LOGGING_DM_MKT_LOADING
        SELECT TO_DATE(s_slice_value, 'yyyymmdd') AS EVENT_DATE, 'CLIENT_PREMIUM' AS DATA_GROUP, d_begin_process, SYSDATE,'DATAMART_MARKETING' AS FLUX_NAME FROM DUAL;
            
        COMMIT;
        
        SELECT COUNT(*) INTO s_nbre_ligne FROM MON.LOGGING_DM_MKT_LOADING WHERE EVENT_DATE = TO_DATE(s_slice_value, 'yyyymmdd') AND FLUX_NAME = 'DATAMART_MARKETING';
        
        IF s_nbre_ligne = nbre_attendu THEN
            -- compression après insertion
            EXECUTE IMMEDIATE 'ALTER TABLE MON.FT_MARKETING_DATAMART MOVE PARTITION MARKETING_DATAMART_' || s_slice_value || ' TABLESPACE TAB_P_MON_J' || SUBSTR(s_slice_value, -2) || '_256M  PCTFREE 0 COMPRESS';
        END IF;
        
    END IF;
END;
/

