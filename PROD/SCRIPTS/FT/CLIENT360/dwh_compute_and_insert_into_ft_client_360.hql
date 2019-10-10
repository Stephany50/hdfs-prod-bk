create PROCEDURE     INSERT_DM_GLOBAL_MANDATORY
/*
    Desc : insertion des donnees globales obligatoires par MSISDN dans le datamart marketing
    Date : 30/11/2015 a 10:16
    Autheur : dimitri.happi@orange.cm
    UPDATE : Ajout champ IMEI par ronny.samo@orange.com le 24/08/2018
*/
    (
        s_slice_value IN VARCHAR2
    ) IS
    s_result VARCHAR2(1500);
    d_begin_process DATE;
    s_nbre_ligne NUMBER;
BEGIN
    d_begin_process := SYSDATE;

    -- Pre-conditions : s'assurer que la table FT_MARKETING_DATAMART est vide de cette journee
    SELECT
      ( CASE
            -- si pas encore traité
            WHEN
                MON.FN_VALIDATE_DAY2DAY_EXIST ('mon.FT_MARKETING_DATAMART', 'EVENT_DATE'
                          , s_slice_value, s_slice_value, 10 ,  '') = 0
                AND
                MON.FN_VALIDATE_DAY2DAY_EXIST ('MON.FT_CONSO_MSISDN_DAY', 'EVENT_DATE', s_slice_value, s_slice_value, 10 ,  '') = 1
                AND
                MON.FN_VALIDATE_DAY2DAY_EXIST ('MON.FT_DATA_CONSO_MSISDN_DAY', 'EVENT_DATE', s_slice_value, s_slice_value, 10 ,  '') = 1
                AND
                MON.FN_VALIDATE_DAY2DAY_EXIST ('MON.FT_SUBSCRIPTION_MSISDN_DAY', 'EVENT_DATE', s_slice_value, s_slice_value, 10 ,  '') = 1
                AND
                MON.FN_VALIDATE_DAY2DAY_EXIST ('MON.FT_CONTRACT_SNAPSHOT', 'EVENT_DATE', TO_CHAR(TO_DATE(s_slice_value, 'yyyymmdd') + 1, 'yyyymmdd'),
                                                                        TO_CHAR(TO_DATE(s_slice_value, 'yyyymmdd') + 1, 'yyyymmdd'), 10 ,  '') = 1
                AND
                MON.FN_VALIDATE_DAY2DAY_EXIST ('MON.FT_ACCOUNT_ACTIVITY', 'EVENT_DATE', TO_CHAR(TO_DATE(s_slice_value, 'yyyymmdd') + 1, 'yyyymmdd'),
                                                                        TO_CHAR(TO_DATE(s_slice_value, 'yyyymmdd') + 1, 'yyyymmdd'), 10 ,  '') = 1
                AND
                MON.FN_VALIDATE_DAY2DAY_EXIST ('MON.FT_SUBSCRIPTION', 'TRANSACTION_DATE', s_slice_value, s_slice_value, 10 ,  '') = 1
            THEN 0
            -- si déjà traité
            ELSE  1
        END ) INTO s_result FROM DUAL;

    IF  s_result = 0 THEN

            -- insertion des donnnees
            INSERT INTO FT_MARKETING_DATAMART

            FULL JOIN
            (
             -- PARTIE 4
                SELECT
                        COALESCE(a.MSISDN, b.MSISDN) AS MSISDN,
                        TOTAL_SUBS_REVENUE,
                        DATA_MAIN_RATED_AMOUNT,
                        DATA_GOS_MAIN_RATED_AMOUNT,
                        DATA_PROMO_RATED_AMOUNT,
                        DATA_ROAM_MAIN_RATED_AMOUNT,
                        DATA_ROAM_PROMO_RATED_AMOUNT,
                        DATA_BYTES_RECEIVED,
                        DATA_BYTES_SENT,
                        DATA_BYTES_USED_IN_BUNDLE,
                        DATA_BYTES_USED_PAYGO,
                        DATA_BYTES_USED_IN_BUNDLE_ROAM,
                        DATA_BYTE_USED_OUT_BUNDLE_ROAM,
                        DATA_MMS_USED,
                        DATA_MMS_USED_IN_BUNDLE,
                        CONTRACT_TYPE,
                        COMMERCIAL_OFFER,
                        ACTIVATION_DATE,
                        DEACTIVATION_DATE,
                        LANG,
                        OSP_STATUS,
                        IMSI,
                        GRP_LAST_OG_CALL,
                        GRP_LAST_IC_CALL,
                        GRP_REMAIN_CREDIT_MAIN,
                        GRP_REMAIN_CREDIT_PROMO,
                        GRP_GP_STATUS,
                        SUBS_BUNDLE_PLENTY_COUNT,
                        SUBS_ORANGE_BONUS_COUNT,
                        SUBS_FNF_MODIFY_COUNT,
                        SUBS_DATA_2G_COUNT,
                        SUBS_DATA_3G_JOUR_COUNT,
                        SUBS_DATA_3G_SEMAINE_COUNT,
                        SUBS_DATA_3G_MOIS_COUNT,
                        SUBS_MAXI_BONUS_COUNT,
                        SUBS_PRO_COUNT,
                        SUBS_INTERN_JOUR_COUNT,
                        SUBS_INTERN_SEMAINE_COUNT,
                        SUBS_INTERN_MOIS_COUNT,
                        SUBS_PACK_SMS_JOUR_COUNT,
                        SUBS_PACK_SMS_SEMAINE_COUNT,
                        SUBS_PACK_SMS_MOIS_COUNT,
                        SUBS_WS_COUNT,
                        SUBS_OBONUS_ILLIMITE_COUNT,
                        SUBS_ORANGE_PHENIX_COUNT,
                        SUBS_RECHARGE_PLENTY_COUNT,
                        SUBS_AUTRES_COUNT,
                        SUBS_DATA_FLYBOX_COUNT,
                        SUBS_DATA_AUTRES_COUNT,
                        SUBS_ROAMING_COUNT,
                        SUBS_SMS_AUTRES_COUNT,
                        SUBS_BUNDLE_PLENTY_AMOUNT,
                        SUBS_ORANGE_BONUS_AMOUNT,
                        SUBS_FNF_MODIFY_AMOUNT,
                        SUBS_DATA_2G_AMOUNT,
                        SUBS_DATA_3G_JOUR_AMOUNT,
                        SUBS_DATA_3G_SEMAINE_AMOUNT,
                        SUBS_DATA_3G_MOIS_AMOUNT,
                        SUBS_MAXI_BONUS_AMOUNT,
                        SUBS_PRO_AMOUNT,
                        SUBS_INTERN_JOUR_AMOUNT,
                        SUBS_INTERN_SEMAINE_AMOUNT,
                        SUBS_INTERN_MOIS_AMOUNT,
                        SUBS_PACK_SMS_JOUR_AMOUNT,
                        SUBS_PACK_SMS_SEMAINE_AMOUNT,
                        SUBS_PACK_SMS_MOIS_AMOUNT,
                        SUBS_WS_AMOUNT,
                        SUBS_OBONUS_ILLIMITE_AMOUNT,
                        SUBS_ORANGE_PHENIX_AMOUNT,
                        SUBS_RECHARGE_PLENTY_AMOUNT,
                        SUBS_AUTRES_AMOUNT,
                        SUBS_DATA_FLYBOX_AMOUNT,
                        SUBS_DATA_AUTRES_AMOUNT,
                        SUBS_ROAMING_AMOUNT,
                        SUBS_SMS_AUTRES_AMOUNT, SUBS_VOICE_COUNT,
                        SUBS_SMS_COUNT,
                        SUBS_DATA_COUNT,
                        SUBS_VOICE_AMOUNT,
                        SUBS_SMS_AMOUNT,
                        SUBS_DATA_AMOUNT
                FROM
                (
                    SELECT
                        COALESCE(a.MSISDN, b.MSISDN) AS MSISDN,
                        TOTAL_SUBS_REVENUE,
                        DATA_MAIN_RATED_AMOUNT,
                        DATA_GOS_MAIN_RATED_AMOUNT,
                        DATA_PROMO_RATED_AMOUNT,
                        DATA_ROAM_MAIN_RATED_AMOUNT,
                        DATA_ROAM_PROMO_RATED_AMOUNT,
                        DATA_BYTES_RECEIVED,
                        DATA_BYTES_SENT,
                        DATA_BYTES_USED_IN_BUNDLE,
                        DATA_BYTES_USED_PAYGO,
                        DATA_BYTES_USED_IN_BUNDLE_ROAM,
                        DATA_BYTE_USED_OUT_BUNDLE_ROAM,
                        DATA_MMS_USED,
                        DATA_MMS_USED_IN_BUNDLE,
                        CONTRACT_TYPE,
                        COMMERCIAL_OFFER,
                        ACTIVATION_DATE,
                        DEACTIVATION_DATE,
                        LANG,
                        OSP_STATUS,
                        IMSI,
                        GRP_LAST_OG_CALL,
                        GRP_LAST_IC_CALL,
                        GRP_REMAIN_CREDIT_MAIN,
                        GRP_REMAIN_CREDIT_PROMO,
                        GRP_GP_STATUS
                    FROM
                    (
                        SELECT MSISDN,
                            COALESCE(MAIN_RATED_AMOUNT, 0) AS DATA_MAIN_RATED_AMOUNT,
                            COALESCE(GOS_DEBIT_AMOUNT, 0) AS DATA_GOS_MAIN_RATED_AMOUNT,
                            COALESCE(PROMO_RATED_AMOUNT, 0) AS DATA_PROMO_RATED_AMOUNT,
                            COALESCE(MAIN_RATED_AMOUNT_ROAMING, 0) AS DATA_ROAM_MAIN_RATED_AMOUNT,
                            COALESCE(PROMO_RATED_AMOUNT_ROAMING, 0) AS DATA_ROAM_PROMO_RATED_AMOUNT,
                            COALESCE(BYTES_RECEIVED, 0) AS DATA_BYTES_RECEIVED,
                            COALESCE(BYTES_SENT, 0) AS DATA_BYTES_SENT,
                            COALESCE(BYTES_USED_IN_BUNDLE, 0) AS DATA_BYTES_USED_IN_BUNDLE,
                            COALESCE(BYTES_USED_OUT_BUNDLE, 0) AS DATA_BYTES_USED_PAYGO,
                            COALESCE(BYTES_USED_IN_BUNDLE_ROAMING, 0) AS DATA_BYTES_USED_IN_BUNDLE_ROAM,
                            COALESCE(BYTES_USED_OUT_BUNDLE_ROAMING, 0) AS DATA_BYTE_USED_OUT_BUNDLE_ROAM,
                            COALESCE(MMS_COUNT, 0) AS DATA_MMS_USED,
                            COALESCE(BUNDLE_MMS_USED_VOLUME, 0) AS DATA_MMS_USED_IN_BUNDLE
                        FROM FT_DATA_CONSO_MSISDN_DAY
                        WHERE EVENT_DATE = TO_DATE(s_slice_value, 'yyyymmdd')
                    ) a
                    FULL JOIN
                    (
                     -- PARTIE 5
                        SELECT
                            COALESCE(a.MSISDN, b.MSISDN) AS MSISDN,
                            TOTAL_SUBS_REVENUE,
                            CONTRACT_TYPE,
                            COMMERCIAL_OFFER,
                            ACTIVATION_DATE,
                            DEACTIVATION_DATE,
                            LANG,
                            OSP_STATUS,
                            IMSI,
                            GRP_LAST_OG_CALL,
                            GRP_LAST_IC_CALL,
                            GRP_REMAIN_CREDIT_MAIN,
                            GRP_REMAIN_CREDIT_PROMO,
                            GRP_GP_STATUS
                        FROM
                        (
                            SELECT MSISDN,
                                COALESCE(TOTAL_SUBS_AMOUNT, 0) AS TOTAL_SUBS_REVENUE
                            FROM FT_SUBSCRIPTION_MSISDN_DAY
                            WHERE EVENT_DATE = TO_DATE(s_slice_value, 'yyyymmdd')
                        ) a
                        FULL JOIN
                        (
                            SELECT COALESCE(a.MSISDN, b.MSISDN) AS MSISDN,
                                CONTRACT_TYPE,
                                COMMERCIAL_OFFER,
                                ACTIVATION_DATE,
                                DEACTIVATION_DATE,
                                LANG,
                                OSP_STATUS,
                                IMSI,
                                GRP_LAST_OG_CALL,
                                GRP_LAST_IC_CALL,
                                GRP_REMAIN_CREDIT_MAIN,
                                GRP_REMAIN_CREDIT_PROMO,
                                GRP_GP_STATUS
                            FROM
                            (
                                -- infos generales
                                SELECT ACCESS_KEY MSISDN,
                                    OSP_CONTRACT_TYPE CONTRACT_TYPE,
                                    COMMERCIAL_OFFER,
                                    ACTIVATION_DATE,
                                    DEACTIVATION_DATE,
                                    LANG,
                                    OSP_STATUS,
                                    MAIN_IMSI IMSI
                                FROM FT_CONTRACT_SNAPSHOT
                                WHERE EVENT_DATE = TO_DATE(s_slice_value, 'yyyymmdd') + 1
                                    AND OSP_STATUS IN ('ACTIVE', 'INACTIVE')
                            ) a
                            FULL JOIN
                            (
                                -- infos groupe
                                SELECT MSISDN,
                                    OG_CALL GRP_LAST_OG_CALL,
                                    IC_CALL_4 GRP_LAST_IC_CALL,
                                    REMAIN_CREDIT_MAIN GRP_REMAIN_CREDIT_MAIN,
                                    REMAIN_CREDIT_PROMO GRP_REMAIN_CREDIT_PROMO,
                                    GP_STATUS GRP_GP_STATUS
                                FROM FT_ACCOUNT_ACTIVITY
                                WHERE EVENT_DATE = TO_DATE(s_slice_value, 'yyyymmdd') + 1
                            ) b
                                ON a.MSISDN = b.MSISDN
                        ) b
                            ON a.MSISDN = b.MSISDN
                    ) b
                        ON a.MSISDN = b.MSISDN
                ) a
                FULL JOIN
                (
                    -- Souscriptions
                    SELECT
                         MSISDN,
                        0 AS SUBS_BUNDLE_PLENTY_COUNT,
                        0 AS SUBS_ORANGE_BONUS_COUNT,
                        0 AS SUBS_FNF_MODIFY_COUNT,
                        0 AS SUBS_DATA_2G_COUNT,
                        0 AS SUBS_DATA_3G_JOUR_COUNT,
                        0 AS SUBS_DATA_3G_SEMAINE_COUNT,
                        0 AS SUBS_DATA_3G_MOIS_COUNT,
                        0 AS SUBS_MAXI_BONUS_COUNT,
                        0 AS SUBS_PRO_COUNT,
                        0 AS SUBS_INTERN_JOUR_COUNT,
                        0 AS SUBS_INTERN_SEMAINE_COUNT,
                        0 AS SUBS_INTERN_MOIS_COUNT,
                        0 AS SUBS_PACK_SMS_JOUR_COUNT,
                        0 AS SUBS_PACK_SMS_SEMAINE_COUNT,
                        0 AS SUBS_PACK_SMS_MOIS_COUNT,
                        0 AS SUBS_WS_COUNT,
                        0 AS SUBS_OBONUS_ILLIMITE_COUNT,
                        0 AS SUBS_ORANGE_PHENIX_COUNT,
                        0 AS SUBS_RECHARGE_PLENTY_COUNT,
                        SUM(CASE WHEN UPPER(USAGE_MKT) = 'AUTRES' THEN NBRE_SUBS ELSE 0 END) AS SUBS_AUTRES_COUNT,
                        0 AS SUBS_DATA_FLYBOX_COUNT,
                        0 AS SUBS_DATA_AUTRES_COUNT,
                        0 AS SUBS_ROAMING_COUNT,
                        0 AS SUBS_SMS_AUTRES_COUNT,
                        0 AS SUBS_BUNDLE_PLENTY_AMOUNT,
                        0 AS SUBS_ORANGE_BONUS_AMOUNT,
                        0 AS SUBS_FNF_MODIFY_AMOUNT,
                        0 AS SUBS_DATA_2G_AMOUNT,
                        0 AS SUBS_DATA_3G_JOUR_AMOUNT,
                        0 AS SUBS_DATA_3G_SEMAINE_AMOUNT,
                        0 AS SUBS_DATA_3G_MOIS_AMOUNT,
                        0 AS SUBS_MAXI_BONUS_AMOUNT,
                        0 AS SUBS_PRO_AMOUNT,
                        0 AS SUBS_INTERN_JOUR_AMOUNT,
						0 AS SUBS_INTERN_SEMAINE_AMOUNT,
						0 AS SUBS_INTERN_MOIS_AMOUNT,
						0 AS SUBS_PACK_SMS_JOUR_AMOUNT,
						0 AS SUBS_PACK_SMS_SEMAINE_AMOUNT,
						0 AS SUBS_PACK_SMS_MOIS_AMOUNT,
						0 AS SUBS_WS_AMOUNT,
						0 AS SUBS_OBONUS_ILLIMITE_AMOUNT,
						0 AS SUBS_ORANGE_PHENIX_AMOUNT,
						0 AS SUBS_RECHARGE_PLENTY_AMOUNT,
						SUM(CASE WHEN UPPER(USAGE_MKT) = 'AUTRES' THEN RATED_AMOUNT ELSE 0 END) AS SUBS_AUTRES_AMOUNT,
						0 AS SUBS_DATA_FLYBOX_AMOUNT,
						0 AS SUBS_DATA_AUTRES_AMOUNT,
						0 AS SUBS_ROAMING_AMOUNT,
						0 AS SUBS_SMS_AUTRES_AMOUNT,
                        SUM(CASE WHEN UPPER(USAGE_MKT) = 'VOIX' THEN NBRE_SUBS ELSE 0 END) AS SUBS_VOICE_COUNT,
                        SUM(CASE WHEN UPPER(USAGE_MKT) = 'SMS' THEN NBRE_SUBS ELSE 0 END) AS SUBS_SMS_COUNT,
                        SUM(CASE WHEN UPPER(USAGE_MKT) = 'DATA' THEN NBRE_SUBS ELSE 0 END) AS SUBS_DATA_COUNT,
                        SUM(CASE WHEN UPPER(USAGE_MKT) = 'VOIX' THEN RATED_AMOUNT ELSE 0 END) AS SUBS_VOICE_AMOUNT,
                        SUM(CASE WHEN UPPER(USAGE_MKT) = 'SMS' THEN RATED_AMOUNT ELSE 0 END) AS SUBS_SMS_AMOUNT,
                        SUM(CASE WHEN UPPER(USAGE_MKT) = 'DATA' THEN RATED_AMOUNT ELSE 0 END) AS SUBS_DATA_AMOUNT
                    FROM
                    (
                        SELECT
							MSISDN,
							COALESCE(USAGE_MKT, 'AUTRES') AS USAGE_MKT,
							SUM(TOTAL_OCCURENCE) AS NBRE_SUBS,
							SUM(RATED_AMOUNT) AS RATED_AMOUNT
						FROM
						(
							SELECT SERVED_PARTY_MSISDN AS MSISDN, SUBSCRIPTION_SERVICE_DETAILS, TOTAL_OCCURENCE, RATED_AMOUNT
							FROM FT_SUBSCRIPTION
							WHERE TRANSACTION_DATE = TO_DATE(s_slice_value, 'yyyymmdd')
						) a
						LEFT JOIN
						(
							SELECT EVENT, USAGE_MKT
							FROM DIM.DT_SERVICES
						) b
							ON a.SUBSCRIPTION_SERVICE_DETAILS = b.EVENT
						GROUP BY MSISDN, COALESCE(USAGE_MKT, 'AUTRES')
                    )
                    GROUP BY MSISDN
                ) b
                    ON a.MSISDN = b.MSISDN
            ) b
                ON a.MSISDN = b.MSISDN;

            -- nettoyage
            DELETE FROM FT_MARKETING_DATAMART WHERE EVENT_DATE = TO_DATE(s_slice_value, 'yyyymmdd') AND MSISDN IS NULL;

            /*******
            -- Logging des groupe de donnees calculees pour cette date
            *******/

            INSERT INTO LOGGING_DM_MKT_LOADING
            SELECT TO_DATE(s_slice_value, 'yyyymmdd') AS EVENT_DATE, 'INFOS_GENERALES' AS DATA_GROUP, d_begin_process, SYSDATE,'DATAMART_MARKETING' AS FLUX_NAME FROM DUAL UNION
            SELECT TO_DATE(s_slice_value, 'yyyymmdd') AS EVENT_DATE, 'INFOS_PARC_GROUPE' AS DATA_GROUP, d_begin_process, SYSDATE,'DATAMART_MARKETING' AS FLUX_NAME FROM DUAL UNION
            SELECT TO_DATE(s_slice_value, 'yyyymmdd') AS EVENT_DATE, 'VOIX-SMS' AS DATA_GROUP, d_begin_process, SYSDATE,'DATAMART_MARKETING' AS FLUX_NAME FROM DUAL UNION
            SELECT TO_DATE(s_slice_value, 'yyyymmdd') AS EVENT_DATE, 'SOUSCRIPTION' AS DATA_GROUP, d_begin_process, SYSDATE,'DATAMART_MARKETING' AS FLUX_NAME FROM DUAL UNION
            SELECT TO_DATE(s_slice_value, 'yyyymmdd') AS EVENT_DATE, 'DATA' AS DATA_GROUP, d_begin_process, SYSDATE,'DATAMART_MARKETING' AS FLUX_NAME FROM DUAL;

            COMMIT;

			-- compression après insertion
			EXECUTE IMMEDIATE 'ALTER TABLE MON.FT_MARKETING_DATAMART MOVE PARTITION MARKETING_DATAMART_' || s_slice_value || ' TABLESPACE TAB_P_MON_J' || SUBSTR(s_slice_value, -2) || '_256M  PCTFREE 0 COMPRESS';


    END IF;
END;
/

