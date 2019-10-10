create PROCEDURE     INSERT_DM_SOS_CREDIT
/*
    Desc : insertion des donnees "SOS credit" des MSISDN dans le datamart marketing
    Date : 25/11/2015 a 12:00
    Autheur : dimitri.happi@orange.cm
*/
    (
        s_slice_value IN VARCHAR2
    ) IS 
    s_result VARCHAR2(1500);
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
                MON.FN_VALIDATE_DAY2DAY_EXIST ('MON.FT_OVERDRAFT', 'TRANSACTION_DATE', s_slice_value, s_slice_value, 10 ,  '') = 1
              AND --// Verification dans la table de logging que les donnees terminal ne sont pas renseignes pour cette date
                NOT EXISTS(SELECT * FROM MON.LOGGING_DM_MKT_LOADING 
                            WHERE EVENT_DATE = TO_DATE(s_slice_value, 'yyyymmdd') AND DATA_GROUP = 'SOS_CREDIT')     
            THEN 0
            -- si déjà traité 
            ELSE  1
        END ) INTO s_result FROM DUAL;
            
    IF  s_result = 0 THEN
        
        MERGE INTO mon.FT_MARKETING_DATAMART a
        USING
            (
                -- Sos credit
                SELECT SERVED_PARTY_MSISDN AS MSISDN,
                    SUM(CASE WHEN OPERATION_TYPE = 'REIMBURSMENT' THEN REFILL_AMOUNT ELSE 0 END) AS SOS_REIMBURSEMENT_AMOUNT,
                    SUM(CASE WHEN OPERATION_TYPE = 'REIMBURSMENT' THEN 1 ELSE 0 END) AS SOS_REIMBURSEMENT_COUNT,
                    SUM(CASE WHEN OPERATION_TYPE = 'LOAN' THEN LOAN_AMOUNT ELSE 0 END) AS SOS_LOAN_AMOUNT,
                    SUM(CASE WHEN OPERATION_TYPE = 'LOAN' THEN 1 ELSE 0 END) AS SOS_LOAN_COUNT,
                    SUM(FEE) AS SOS_FEES
                FROM FT_OVERDRAFT
                WHERE TRANSACTION_DATE >= TO_DATE(s_slice_value, 'yyyymmdd') AND TRANSACTION_DATE < TO_DATE(s_slice_value, 'yyyymmdd') + 1
                GROUP BY SERVED_PARTY_MSISDN
            ) b
                ON (a.MSISDN = b.MSISDN AND a.EVENT_DATE = TO_DATE(s_slice_value, 'yyyymmdd'))
         WHEN MATCHED THEN
            UPDATE SET a.SOS_REIMBURSEMENT_AMOUNT = b.SOS_REIMBURSEMENT_AMOUNT, a.SOS_REIMBURSEMENT_COUNT = b.SOS_REIMBURSEMENT_COUNT, 
                       a.SOS_LOAN_AMOUNT = b.SOS_LOAN_AMOUNT,
                       a.SOS_LOAN_COUNT = b.SOS_LOAN_COUNT, a.SOS_FEES = b.SOS_FEES,
                       a.REFRESH_DATE = SYSDATE
                   WHERE COALESCE(a.SOS_REIMBURSEMENT_AMOUNT,-1) <> COALESCE(b.SOS_REIMBURSEMENT_AMOUNT,-1)
                        OR COALESCE(a.SOS_REIMBURSEMENT_COUNT,-1) <> COALESCE(b.SOS_REIMBURSEMENT_COUNT,-1)
                        OR COALESCE(a.SOS_LOAN_AMOUNT,-1) <> COALESCE(b.SOS_LOAN_AMOUNT,-1)
                        OR COALESCE(a.SOS_LOAN_COUNT,-1) <> COALESCE(b.SOS_LOAN_COUNT,-1)
                        OR COALESCE(a.SOS_FEES,-1) <> COALESCE(b.SOS_FEES,-1);
                       
        COMMIT;
        
        /*******
        -- Logging du groupe de donnees SOS credit calculees pour cette date 
        *******/
            
        INSERT INTO LOGGING_DM_MKT_LOADING
        SELECT TO_DATE(s_slice_value, 'yyyymmdd') AS EVENT_DATE, 'SOS_CREDIT' AS DATA_GROUP, d_begin_process, SYSDATE,'DATAMART_MARKETING' AS FLUX_NAME FROM DUAL;
            
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

