create PROCEDURE     INSERT_DM_RECHARGE_ET_P2P
/*
    Desc : insertion des donnees "Recharge et P2P" des MSISDN dans le datamart marketing
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
                MON.FN_VALIDATE_DAY2DAY_EXIST ('MON.FT_REFILL', 'REFILL_DATE', s_slice_value, s_slice_value, 2 ,  ' AND REFILL_MEAN = ''C2S''') = 1
                AND CASE WHEN s_slice_value BETWEEN 20181202 AND 20181209 THEN 1 ELSE 
                MON.FN_VALIDATE_DAY2DAY_EXIST ('MON.FT_REFILL', 'REFILL_DATE', s_slice_value, s_slice_value, 2 ,  ' AND REFILL_MEAN = ''SCRATCH''') END = 1 
            AND
                MON.FN_VALIDATE_DAY2DAY_EXIST ('MON.FT_CREDIT_TRANSFER', 'REFILL_DATE', s_slice_value, s_slice_value, 10 ,  '') = 1
              AND --// Verification dans la table de logging que les donnees terminal ne sont pas renseignes pour cette date
                NOT EXISTS(SELECT * FROM MON.LOGGING_DM_MKT_LOADING 
                            WHERE EVENT_DATE = TO_DATE(s_slice_value, 'yyyymmdd') AND DATA_GROUP = 'RECHARGE_ET_P2P')     
            THEN 0
            -- si déjà traité 
            ELSE  1
        END ) INTO s_result FROM DUAL;
            
    IF  s_result = 0 THEN
        
        MERGE INTO mon.FT_MARKETING_DATAMART a
        USING
            (
                -- recharge et P2P
                SELECT 
                    COALESCE(a.MSISDN, b.MSISDN) AS MSISDN,
                    COALESCE(C2S_REFILL_COUNT, 0) AS C2S_REFILL_COUNT,
                    COALESCE(C2S_MAIN_REFILL_AMOUNT, 0) AS C2S_MAIN_REFILL_AMOUNT,
                    COALESCE(C2S_PROMO_REFILL_AMOUNT, 0) AS C2S_PROMO_REFILL_AMOUNT,
                    COALESCE(P2P_REFILL_COUNT, 0) AS P2P_REFILL_COUNT,
                    COALESCE(P2P_REFILL_AMOUNT, 0) AS P2P_REFILL_AMOUNT,
                    COALESCE(SCRATCH_REFILL_COUNT, 0) AS SCRATCH_REFILL_COUNT,
                    COALESCE(SCRATCH_MAIN_REFILL_AMOUNT, 0) AS SCRATCH_MAIN_REFILL_AMOUNT,
                    COALESCE(SCRATCH_PROMO_REFILL_AMOUNT, 0) AS SCRATCH_PROMO_REFILL_AMOUNT,
                    COALESCE(P2P_REFILL_FEES, 0) AS P2P_REFILL_FEES
                FROM
                (
                    --C2S et SCRATCH
                    SELECT 
                        RECEIVER_MSISDN MSISDN
                        ,SUM(CASE WHEN REFILL_MEAN='C2S' THEN 1 ELSE 0 END ) C2S_REFILL_COUNT
                        ,SUM(CASE WHEN REFILL_MEAN='C2S' THEN REFILL_AMOUNT ELSE 0 END ) C2S_MAIN_REFILL_AMOUNT
                        ,SUM(CASE WHEN REFILL_MEAN='C2S' THEN REFILL_BONUS ELSE 0 END ) C2S_PROMO_REFILL_AMOUNT
                        ,SUM(CASE WHEN REFILL_MEAN='SCRATCH' THEN 1 ELSE 0 end ) SCRATCH_REFILL_COUNT
                        ,SUM(CASE WHEN REFILL_MEAN='SCRATCH' THEN REFILL_AMOUNT ELSE 0 END ) SCRATCH_MAIN_REFILL_AMOUNT
                        ,SUM(CASE WHEN REFILL_MEAN='SCRATCH' THEN REFILL_BONUS ELSE 0 END ) SCRATCH_PROMO_REFILL_AMOUNT
                    FROM FT_REFILL
                    WHERE REFILL_DATE = TO_DATE(s_slice_value, 'yyyymmdd')
                        AND TERMINATION_IND='200'
                    GROUP BY RECEIVER_MSISDN
                ) a
                FULL JOIN
                (
                    --P2P
                    SELECT 
                        RECEIVER_MSISDN MSISDN
                        ,COUNT(*) P2P_REFILL_COUNT
                        ,SUM(TRANSFER_AMT) P2P_REFILL_AMOUNT
                        ,SUM(TRANSFER_FEES) P2P_REFILL_FEES
                    FROM FT_CREDIT_TRANSFER
                    WHERE REFILL_DATE = TO_DATE(s_slice_value, 'yyyymmdd')
                        AND TERMINATION_IND='000'
                    GROUP BY RECEIVER_MSISDN
                ) b
                    ON a.MSISDN = b.MSISDN
            ) b
                ON (a.MSISDN = b.MSISDN AND a.EVENT_DATE = TO_DATE(s_slice_value, 'yyyymmdd'))
         WHEN MATCHED THEN
            UPDATE SET a.C2S_REFILL_COUNT = b.C2S_REFILL_COUNT, a.C2S_MAIN_REFILL_AMOUNT = b.C2S_MAIN_REFILL_AMOUNT, a.C2S_PROMO_REFILL_AMOUNT = b.C2S_PROMO_REFILL_AMOUNT,
                       a.P2P_REFILL_COUNT = b.P2P_REFILL_COUNT, a.P2P_REFILL_AMOUNT = b.P2P_REFILL_AMOUNT, a.P2P_REFILL_FEES = b.P2P_REFILL_FEES,
                       a.SCRATCH_REFILL_COUNT = b.SCRATCH_REFILL_COUNT , a.SCRATCH_MAIN_REFILL_AMOUNT = b.SCRATCH_MAIN_REFILL_AMOUNT ,
                       a.SCRATCH_PROMO_REFILL_AMOUNT = b.SCRATCH_PROMO_REFILL_AMOUNT , 
                       a.REFRESH_DATE = SYSDATE
                   WHERE COALESCE(a.C2S_REFILL_COUNT,-1) <> COALESCE(b.C2S_REFILL_COUNT,-1)
                            OR COALESCE(a.C2S_MAIN_REFILL_AMOUNT,-1) <> COALESCE(b.C2S_MAIN_REFILL_AMOUNT,-1)
                            OR COALESCE(a.P2P_REFILL_COUNT,-1) <> COALESCE(b.P2P_REFILL_COUNT,-1)
                            OR COALESCE(a.P2P_REFILL_AMOUNT,-1) <> COALESCE(b.P2P_REFILL_AMOUNT,-1)
                            OR COALESCE(a.P2P_REFILL_FEES,-1) <> COALESCE(b.P2P_REFILL_FEES,-1)
                            OR COALESCE(a.SCRATCH_REFILL_COUNT,-1) <> COALESCE(b.SCRATCH_REFILL_COUNT,-1)
                            OR COALESCE(a.SCRATCH_MAIN_REFILL_AMOUNT,-1) <> COALESCE(b.SCRATCH_MAIN_REFILL_AMOUNT,-1)
                            OR COALESCE(a.SCRATCH_PROMO_REFILL_AMOUNT,-1) <> COALESCE(b.SCRATCH_PROMO_REFILL_AMOUNT,-1);
                       
        COMMIT;
        
        /*******
        -- Logging du groupe de donnees "Recharge et P2P" calculees pour cette date 
        *******/
            
        INSERT INTO LOGGING_DM_MKT_LOADING
        SELECT TO_DATE(s_slice_value, 'yyyymmdd') AS EVENT_DATE, 'RECHARGE_ET_P2P' AS DATA_GROUP, d_begin_process, SYSDATE,'DATAMART_MARKETING' AS FLUX_NAME FROM DUAL;
            
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

