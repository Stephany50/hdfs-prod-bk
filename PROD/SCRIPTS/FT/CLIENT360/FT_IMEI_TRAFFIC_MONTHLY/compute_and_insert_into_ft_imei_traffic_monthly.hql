create FUNCTION     FN_IMEI_TRAFFIC_MONTHLY (p_slice_value VARCHAR2) RETURN VARCHAR2 IS
-- @param: p_slice_value : month code (yyyymm) courant  representant le mois de trafic à traiter
-- @version v1.001.2014-02-24_16!02 by cyrille.kouam@orange.com
-- @desc : agreger les données de trafic mensuel d'imei par msisdn (avec détails sur msisdn en fin de mois)
-- @sample : mon.FN_IMEI_TRAFFIC_MONTHLY (s_slice_value)
        s_return_code VARCHAR2(10) := '';
        s_function_name VARCHAR2(30) := 'FN_IMEI_TRAFFIC_MONTHLY';
        --
        n_sqlcode  NUMBER(38);
        s_sqlerrm VARCHAR2(250);
        s_task_name VARCHAR2(250);
        s_sql_query VARCHAR2(1500);
        s_file_name VARCHAR2(250);
        --
        d_result DATE;
        s_result VARCHAR2(250);
        n_result NUMBER ;
        --
        n_is_curr_slice_already_done NUMBER := 0;
        n_is_curr_slice_can_be_done NUMBER := 0;
        --
        s_slice_value VARCHAR2(20) := TRIM (p_slice_value);
        d_slice_value DATE := NULL ;
        --
        d_month_begin_date DATE := NULL ;
        d_month_end_date DATE := NULL ;
        --
     BEGIN

        -- ###OK_FORCED_RETURN### => exception qui ne generera pas de mail d'alerte, juste arrêter immediatement les traitements, une peu plus fort que RETURN 'NOK'
        -- ###ERROR### => exception qui generera un mail d'alerte, erreur inattendue au courant du traitement

        -- @todo : implementer le principe de semaphore car ce calcul utilise des tables temporaires ne supportant pas de manipulation parallelle voire concurrencielles


        -- INITIALISER
            d_slice_value :=  ADD_MONTHS (TO_DATE (s_slice_value ||  '01', 'yyyymmdd') , 1);  -- TO_DATE (s_slice_value || '01', 'yyyymmdd');
            --
            d_month_begin_date :=  TO_DATE (s_slice_value || '01', 'yyyymmdd');
            d_month_end_date := (ADD_MONTHS (TO_DATE (s_slice_value ||  '01', 'yyyymmdd') , 1) - 1) ;


        -- VERIFIER:: COHERENCE DES PARAMS ?
            IF ((s_slice_value IS NULL) OR (d_slice_value IS NULL)  OR (d_month_begin_date IS NULL)   OR (d_month_end_date IS NULL) )  THEN
                RAISE_APPLICATION_ERROR (-20000,'###ERROR### . cause:  s_slice_value IS NULL ');
                RETURN 'NOK';
            END IF ;

            -- VERIFIER:: TRANCHE HORAIRE D'EXECUTION PERMISE ?
                    SELECT
                        ( CASE
                            -- si ok
                            WHEN
                                  SYSDATE > (TO_DATE (TO_CHAR (d_slice_value, 'yyyymmdd') || ' 113000', 'yyyymmdd hh24miss')  + 0)
                             THEN 1
                             -- si nok
                            ELSE  0
                        END ) svalue INTO n_is_curr_slice_can_be_done FROM DUAL
                        ;
                    -- si le calcule pour slice ne peut être effectué
                    IF n_is_curr_slice_can_be_done <> 1 THEN
                        -- imposer un retour avec message
                        -- RAISE_APPLICATION_ERROR (-20000,' ###OK_FORCED_RETURN###');
                        --
                        RETURN 'NOK';
                    END IF ;


            -- EMPECHER LES DOUBLONS <FT_CONSO_MSISDN_MONTH>
                n_is_curr_slice_already_done := 0;
                -- verif afin de simplement arrêter (sans generer d'erreur) l'exec du script courant si la journée demandée a déjà été calculer
                    SELECT
                          ( CASE
                                -- si déjà traité 'OK'
                                WHEN
                                      MON.FN_VALIDATE_MONTH2MONTH_EXIST ('mon.FT_IMEI_TRAFFIC_MONTHLY', 'SMONTH'
                                              , s_slice_value, s_slice_value, 10  ) = 1
                                THEN 1
                                -- si pas encore traité 'NOK'
                                ELSE  0
                            END ) svalue INTO n_is_curr_slice_already_done FROM DUAL
                            ;
                    -- si le slice est déjà calculer alors simplement sortir
                        IF n_is_curr_slice_already_done = 1 THEN
                            -- imposer un retour avec message
                            --  car journée déjà calculer => ne pas lever d'exception ::  RAISE_APPLICATION_ERROR (-20000,' ###OK_FORCED_RETURN###');
                            --
                            RETURN 'OK';
                        END IF ;


        -- VERIFIER:: jour <MON.FT_IMEI_ONLINE> NECESSAIRE EST BIEN RENSEIGNÉE ?
            SELECT
                ( CASE
                    -- si ok
                    WHEN
                          MON.FN_VALIDATE_DAY2DAY_EXIST ('mon.FT_IMEI_ONLINE', 'SDATE'
                                              , TO_CHAR (d_month_begin_date, 'yyyymmdd') , TO_CHAR (d_month_end_date, 'yyyymmdd'), 10 ) = 1
                     THEN 1
                     -- si nok
                    ELSE  0
                END ) svalue INTO n_is_curr_slice_can_be_done FROM DUAL
                ;
            -- si le calcule pour slice ne peut être effectué
            IF n_is_curr_slice_can_be_done <> 1 THEN
                -- imposer un retour avec message
                -- RAISE_APPLICATION_ERROR (-20000,' ###OK_FORCED_RETURN###');
                --
                RETURN 'NOK';
            END IF ;


        -- VERIFIER:: jour <MON.FT_CONTRACT_SNAPSHOT> NECESSAIRE EST BIEN RENSEIGNÉE ?
            SELECT
                ( CASE
                    -- si ok
                    WHEN
                          MON.FN_VALIDATE_DAY2DAY_EXIST ('mon.FT_CONTRACT_SNAPSHOT', 'EVENT_DATE'
                                              , TO_CHAR (ADD_MONTHS (TO_DATE (s_slice_value || '01', 'yyyymmdd'), 1), 'yyyymmdd')
                                              , TO_CHAR (ADD_MONTHS (TO_DATE (s_slice_value || '01', 'yyyymmdd'), 1), 'yyyymmdd')
                                              , 10) = 1
                     THEN 1
                     -- si nok
                    ELSE  0
                END ) svalue INTO n_is_curr_slice_can_be_done FROM DUAL
                ;
            -- si le calcule pour slice ne peut être effectué
            IF n_is_curr_slice_can_be_done <> 1 THEN
                -- imposer un retour avec message
                -- RAISE_APPLICATION_ERROR (-20000,' ###OK_FORCED_RETURN###');
                --
                RETURN 'NOK';
            END IF ;


        -- @TRAITEMENT :: RECUPERER LES DONNÉES  ET  INSERER LES DONNÉES AGREGÉES <FT_CONSO_MSISDN_MONTH>
            --
               INSERT INTO MON.FT_IMEI_TRAFFIC_MONTHLY
            SELECT smonth, imei, imsi, msisdn
                , nvl(b.PROFILE_CODE,'') profile_code
                , nvl(b.PROFILE_NAME,'') profile_name
                , NVL (a.LANG, 'FR') language
                , DECODE (NVL(a.OSP_STATUS, nvl(a.CURRENT_STATUS,'')), 'ACTIVE', 'ACTIVE', 'a' , 'ACTIVE', 'd', 'DEACT'
                        , 's', 'INACTIVE', 'DEACTIVATED', 'DEACT', 'INACTIVE', 'INACTIVE', 'VALID', 'VALID'
                        , NVL(a.OSP_STATUS, nvl(a.CURRENT_STATUS,''))) status
                , date_first_usage, date_last_usage, total_days_count
                , 'FT_IMEI_ONLINE' SRC_TABLE
                , TRUNC (SYSDATE) INSERT_DATE
                , nvl(a.ACTIVATION_DATE,null) ACTIVATION_DATE
            FROM MON.FT_CONTRACT_SNAPSHOT a
                 , (
                    SELECT b.PROFILE_CODE,  MAX (TRIM (b.CUSTOMER_TYPE)) CUSTOMER_TYPE, MAX (TRIM (b.OFFER_NAME))  OFFER_NAME
                        , MAX (TRIM (b.PROFILE_NAME)) PROFILE_NAME, MAX (TRIM (b.CRM_SEGMENTATION)) CRM_SEGMENTATION, MAX (TRIM (b.CUSTOMER_PROFILE)) CUSTOMER_PROFILE
                        , MAX (TRIM (DECILE_TYPE)) DECILE_TYPE
                        FROM  DIM.DT_OFFER_PROFILES b
                        GROUP BY b.PROFILE_CODE
                    ) b
                , (SELECT s_slice_value smonth, substr(imei, 1, 14) imei, imsi, msisdn  --M@J:20170710 Restriction au 14 premier caractere de imei G2d.
                        , min(sdate) date_first_usage, max(sdate) date_last_usage
                        , count(DISTINCT sdate) total_days_count
                        , 'FT_IMEI_ONLINE' SRC_TABLE
                        , TRUNC (SYSDATE) INSERT_DATE
                    FROM MON.FT_IMEI_ONLINE
                    WHERE sdate BETWEEN d_month_begin_date AND d_month_end_date
                    GROUP BY substr(imei, 1, 14), imsi, msisdn
                ) c
            WHERE c.MSISDN = a.ACCESS_KEY(+)
                AND a.EVENT_DATE = d_month_end_date + 1
                AND UPPER (NVL(a.PROFILE,  substr(a.BSCS_COMM_OFFER, instr(a.BSCS_COMM_OFFER,'|',1,1)+1))) = b.PROFILE_CODE
                AND DECODE (NVL(a.OSP_STATUS, a.CURRENT_STATUS), 'ACTIVE', 'ACTIVE', 'a' , 'ACTIVE', 'd', 'DEACT'
                        , 's', 'INACTIVE', 'DEACTIVATED', 'DEACT', 'INACTIVE', 'INACTIVE', 'VALID', 'VALID'
                            , NVL(a.OSP_STATUS, a.CURRENT_STATUS)) <> 'TERMINATED'
            ;
            COMMIT;


        -- FIN
        COMMIT;

        RETURN 'OK';


     END ; -- end function

-- #END_REGION CREATE OR REPLACE FUNCTION FN_DO_CONSO_MSISDN_MONTH
/

