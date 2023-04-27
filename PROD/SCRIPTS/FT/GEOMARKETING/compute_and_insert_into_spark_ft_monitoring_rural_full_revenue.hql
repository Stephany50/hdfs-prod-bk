INSERT INTO MON.SPARK_FT_MONITORING_RURAL_FULL_REVENUE
SELECT 
    UPPER(TRIM(
        COALESCE(T.SITE_NAME, A.SITE_NAME, R.SITE_NAME, E.SITE_NAME, F.SITE_NAME, F1.SITE_NAME, G.SITE_NAME, H.SITE_NAME, P2.SITE_NAME, J.SITE_NAME, O.SITE_NAME, P.SITE_NAME, Q.SITE_NAME) 
    )) SITE_NAME,
    revenu_voix_paygo,
    revenu_sms_paygo,
    revenu_data_paygo,
    revenu_voix_subs,
    revenu_sms_subs,
    revenu_data_subs,
    revenu_credit_compte_desactive,
    EMERGENCY_DATA,
    revenu_vas_retail,
    revenu_p2p_voix, -- p2p voix
    revenu_p2p_data, -- p2p data
    revenu_data_roaming,
    parc_group,
    charged_base,
    data_users,
    gross_add,
    gross_add_data,
    gross_add_om,
    call_box,
    pos_om,
    famoco,
    CURRENT_TIMESTAMP INSERT_DATE,
    '###SLICE_VALUE###' EVENT_DATE
FROM
(
    SELECT
        DISTINCT SITE_NAME
    FROM MON.SPARK_FT_GEOMARKETING_REPORT_360
    WHERE EVENT_DATE = '###SLICE_VALUE###'
) T
FULL JOIN
(
    SELECT SITE_NAME,
        SUM(REVENU_VOIX_PAYGO) REVENU_VOIX_PAYGO,
        SUM(REVENU_SMS_PAYGO) REVENU_SMS_PAYGO,
        SUM(REVENU_DATA_PAYGO) REVENU_DATA_PAYGO,
        SUM(REVENU_VOIX_SUBS) REVENU_VOIX_SUBS,
        SUM(REVENU_DATA_SUBS) REVENU_DATA_SUBS,
        SUM(REVENU_SMS_SUBS) REVENU_SMS_SUBS,
        SUM(EMERGENCY_DATA) EMERGENCY_DATA,
        SUM(revenu_vas_retail) revenu_vas_retail,
        SUM(revenu_data_roaming) revenu_data_roaming,
        sum(revenu_credit_compte_desactive) revenu_credit_compte_desactive,
        SUM(revenu_p2p_voix) revenu_p2p_voix,
        sum(revenu_p2p_data) revenu_p2p_data
    FROM
    (
        SELECT NVL(A000.MSISDN, A001.MSISDN) msisdn,
            MAX(SITE_NAME) SITE_NAME,
            SUM(REVENU_VOIX_PAYGO) REVENU_VOIX_PAYGO,
            SUM(REVENU_SMS_PAYGO) REVENU_SMS_PAYGO,
            SUM(REVENU_DATA_PAYGO) REVENU_DATA_PAYGO,
            SUM(REVENU_VOIX_SUBS) REVENU_VOIX_SUBS,
            SUM(REVENU_DATA_SUBS) REVENU_DATA_SUBS,
            SUM(REVENU_SMS_SUBS) REVENU_SMS_SUBS,
            SUM(EMERGENCY_DATA) EMERGENCY_DATA,
            SUM(revenu_vas_retail) revenu_vas_retail,
            SUM(revenu_data_roaming) revenu_data_roaming,
            sum(revenu_credit_compte_desactive) revenu_credit_compte_desactive,
            SUM(revenu_p2p_voix) revenu_p2p_voix,
            sum(revenu_p2p_data) revenu_p2p_data
        FROM
        (
            SELECT
                MSISDN,
                SUM(NVL(REVENU_VOIX_PAYGO,0))REVENU_VOIX_PAYGO,
                SUM(NVL(REVENU_SMS_PAYGO,0))REVENU_SMS_PAYGO,
                SUM(0)REVENU_DATA_PAYGO,
                SUM(0)REVENU_VOIX_SUBS,
                SUM(0)REVENU_DATA_SUBS,
                SUM(0) REVENU_SMS_SUBS,
                SUM(0) EMERGENCY_DATA,
                sum(0) revenu_vas_retail,
                sum(0) revenu_data_roaming,
                sum(0) revenu_credit_compte_desactive,
                SUM(0) revenu_p2p_voix,
                sum(0) revenu_p2p_data 
            FROM 
            (
                SELECT 
                    CHARGED_PARTY MSISDN,
                    NVL(SUM(CALL_PROCESS_TOTAL_DURATION/60), 0) TRAFIC_VOIX, 
                    SUM(0) TRAFIC_DATA,
                    NVL(SUM(
                        CASE 
                            WHEN 
                                (
                                    CASE
                                    WHEN SERVICE_CODE = 'SMS' THEN 'NVX_SMS'
                                    WHEN SERVICE_CODE = 'TEL' THEN 'VOI_VOX'
                                    WHEN SERVICE_CODE = 'USS' THEN 'NVX_USS'
                                    WHEN SERVICE_CODE = 'GPR' THEN 'NVX_DAT_GPR'
                                    WHEN SERVICE_CODE = 'DFX' THEN 'NVX_DFX'
                                    WHEN SERVICE_CODE = 'DAT' THEN 'NVX_DAT'
                                    WHEN SERVICE_CODE = 'VDT' THEN 'NVX_VDT'
                                    WHEN SERVICE_CODE = 'WEB' THEN 'NVX_WEB'
                                    WHEN UPPER(SERVICE_CODE) IN ('SMSMO','SMSRMG') THEN 'NVX_SMS'
                                    WHEN UPPER(SERVICE_CODE) IN ('OC','OCFWD','OCRMG','TCRMG') THEN 'VOI_VOX'
                                    WHEN UPPER(SERVICE_CODE) LIKE '%FNF%MODIFICATION%' THEN 'VOI_VOX'
                                    WHEN UPPER(SERVICE_CODE) LIKE '%ACCOUNT%INTERRO%' THEN 'VOI_VOX'
                                    ELSE 'AUT'
                                    END
                                ) IN ('NVX_SMS')
                                AND 
                                (
                                    CASE 
                                    WHEN Call_Destination_Code IN ('ONNET','ONNETFREE','OCM_D') THEN 'OUT_NAT_MOB_OCM'
                                    WHEN Call_Destination_Code IN ('MTN','MTN_D') THEN 'OUT_NAT_MOB_MTN'
                                    WHEN Call_Destination_Code IN ('CAM_D','CAM') THEN 'OUT_NAT_MOB_CAM'
                                    WHEN Call_Destination_Code IN ('NEXTTEL','NEXTTEL_D') THEN 'OUT_NAT_MOB_NEX'
                                    WHEN Call_Destination_Code = 'VAS' THEN 'OUT_SVA'
                                    WHEN Call_Destination_Code = 'EMERG' THEN 'OUT_CCSVA'
                                    WHEN Call_Destination_Code = 'OCRMG' THEN 'OUT_ROAM_MO'
                                    WHEN Call_Destination_Code = 'TCRMG' THEN 'IN_ROAM_MT'
                                    WHEN Call_Destination_Code = 'INT' THEN 'OUT_INT'
                                    WHEN Call_Destination_Code = 'MVNO' THEN 'OUT_NAT_MOB_MVO'
                                    ELSE Call_Destination_Code END
                                ) IN ('OUT_NAT_MOB_OCM', 'OUT_NAT_MOB_MTN', 'OUT_NAT_MOB_NEX', 'OUT_NAT_MOB_CAM', 'OUT_ROAM_MO')
                            THEN 1
                        ELSE 0 END
                    ), 0) TRAFIC_SMS,
                    NVL(SUM (
                        CASE 
                            WHEN 
                                (
                                    CASE
                                    WHEN SERVICE_CODE = 'SMS' THEN 'NVX_SMS'
                                    WHEN SERVICE_CODE = 'TEL' THEN 'VOI_VOX'
                                    WHEN SERVICE_CODE = 'USS' THEN 'NVX_USS'
                                    WHEN SERVICE_CODE = 'GPR' THEN 'NVX_DAT_GPR'
                                    WHEN SERVICE_CODE = 'DFX' THEN 'NVX_DFX'
                                    WHEN SERVICE_CODE = 'DAT' THEN 'NVX_DAT'
                                    WHEN SERVICE_CODE = 'VDT' THEN 'NVX_VDT'
                                    WHEN SERVICE_CODE = 'WEB' THEN 'NVX_WEB'
                                    WHEN UPPER(SERVICE_CODE) IN ('SMSMO','SMSRMG') THEN 'NVX_SMS'
                                    WHEN UPPER(SERVICE_CODE) IN ('OC','OCFWD','OCRMG','TCRMG') THEN 'VOI_VOX'
                                    WHEN UPPER(SERVICE_CODE) LIKE '%FNF%MODIFICATION%' THEN 'VOI_VOX'
                                    WHEN UPPER(SERVICE_CODE) LIKE '%ACCOUNT%INTERRO%' THEN 'VOI_VOX'
                                    ELSE 'AUT'
                                    END
                                ) IN ('VOI_VOX') THEN MAIN_RATED_AMOUNT 
                            ELSE  0  END 
                    ), 0) REVENU_VOIX_PAYGO,
                    NVL((SUM (MAIN_RATED_AMOUNT) - 
                        (SUM 
                        (CASE 
                            WHEN 
                            (
                                CASE
                                WHEN SERVICE_CODE = 'SMS' THEN 'NVX_SMS'
                                WHEN SERVICE_CODE = 'TEL' THEN 'VOI_VOX'
                                WHEN SERVICE_CODE = 'USS' THEN 'NVX_USS'
                                WHEN SERVICE_CODE = 'GPR' THEN 'NVX_DAT_GPR'
                                WHEN SERVICE_CODE = 'DFX' THEN 'NVX_DFX'
                                WHEN SERVICE_CODE = 'DAT' THEN 'NVX_DAT'
                                WHEN SERVICE_CODE = 'VDT' THEN 'NVX_VDT'
                                WHEN SERVICE_CODE = 'WEB' THEN 'NVX_WEB'
                                WHEN UPPER(SERVICE_CODE) IN ('SMSMO','SMSRMG') THEN 'NVX_SMS'
                                WHEN UPPER(SERVICE_CODE) IN ('OC','OCFWD','OCRMG','TCRMG') THEN 'VOI_VOX'
                                WHEN UPPER(SERVICE_CODE) LIKE '%FNF%MODIFICATION%' THEN 'VOI_VOX'
                                WHEN UPPER(SERVICE_CODE) LIKE '%ACCOUNT%INTERRO%' THEN 'VOI_VOX'
                                ELSE 'AUT'
                                END
                            ) IN ('VOI_VOX') THEN MAIN_RATED_AMOUNT ELSE 0 END 
                        ) +
                        SUM 
                        (CASE 
                            WHEN 
                            (
                                CASE
                                WHEN SERVICE_CODE = 'SMS' THEN 'NVX_SMS'
                                WHEN SERVICE_CODE = 'TEL' THEN 'VOI_VOX'
                                WHEN SERVICE_CODE = 'USS' THEN 'NVX_USS'
                                WHEN SERVICE_CODE = 'GPR' THEN 'NVX_DAT_GPR'
                                WHEN SERVICE_CODE = 'DFX' THEN 'NVX_DFX'
                                WHEN SERVICE_CODE = 'DAT' THEN 'NVX_DAT'
                                WHEN SERVICE_CODE = 'VDT' THEN 'NVX_VDT'
                                WHEN SERVICE_CODE = 'WEB' THEN 'NVX_WEB'
                                WHEN UPPER(SERVICE_CODE) IN ('SMSMO','SMSRMG') THEN 'NVX_SMS'
                                WHEN UPPER(SERVICE_CODE) IN ('OC','OCFWD','OCRMG','TCRMG') THEN 'VOI_VOX'
                                WHEN UPPER(SERVICE_CODE) LIKE '%FNF%MODIFICATION%' THEN 'VOI_VOX'
                                WHEN UPPER(SERVICE_CODE) LIKE '%ACCOUNT%INTERRO%' THEN 'VOI_VOX'
                                ELSE 'AUT'
                                END
                            ) NOT IN ('NVX_SMS','VOI_VOX') THEN MAIN_RATED_AMOUNT 
                        ELSE 0 END ))
                    ), 0) REVENU_SMS_PAYGO
                FROM MON.SPARK_FT_BILLED_TRANSACTION_PREPAID
                WHERE TRANSACTION_DATE='###SLICE_VALUE###'
                    AND MAIN_RATED_AMOUNT >= 0
                    AND PROMO_RATED_AMOUNT >= 0
                GROUP BY 
                    CHARGED_PARTY

                UNION ALL

                SELECT
                    CHARGED_PARTY_MSISDN MSISDN,
                    SUM(0) TRAFIC_VOIX,
                    NVL(SUM(NVL(BYTES_RECEIVED,0) + NVL(BYTES_SENT,0))/1024/1024, 0) TRAFIC_DATA, 
                    SUM(0) TRAFIC_SMS,
                    SUM(0) REVENU_VOIX_PAYGO,
                    SUM(0) REVENU_SMS_PAYGO
                FROM MON.SPARK_FT_CRA_GPRS
                WHERE SESSION_DATE='###SLICE_VALUE###'
                AND NVL(MAIN_COST, 0) >= 0
                GROUP BY 
                    CHARGED_PARTY_MSISDN
            ) T
            GROUP BY MSISDN

            UNION ALL

            SELECT 
                MSISDN, 
                SUM(0) REVENU_VOIX_PAYGO, 
                SUM(0) REVENU_DATA_PAYGO, 
                SUM(0) REVENU_SMS_PAYGO,  
                SUM(NVL(REVENU_SUBS*COEF_VOIX,0)) REVENU_VOIX_SUBS,
                SUM(NVL(REVENU_SUBS*COEF_DATA,0)) REVENU_DATA_SUBS,
                SUM(NVL(REVENU_SUBS*COEF_SMS,0)) REVENU_SMS_SUBS,
                SUM(0) EMERGENCY_DATA,
                sum(0) revenu_vas_retail,
                sum(0) revenu_data_roaming,
                sum(0) revenu_credit_compte_desactive,
                SUM(0) revenu_p2p_voix,
                sum(0) revenu_p2p_data 
            FROM 
            ( 
                SELECT 
                    SERVED_PARTY_MSISDN MSISDN,
                    SUBSCRIPTION_SERVICE_DETAILS,
                    NVL(RATED_AMOUNT,0)REVENU_SUBS
                FROM MON.SPARK_FT_SUBSCRIPTION 
                WHERE TRANSACTION_DATE='###SLICE_VALUE###'
            )A
            INNER JOIN 
            (   SELECT 
                    BDLE_NAME,
                    SUM(NVL(COEFF_ONNET,0)+NVL(COEFF_OFFNET,0)+NVL(COEFF_ROAMING_VOIX,0)+NVL(COEFF_INTER,0))/100 COEF_VOIX,
                    SUM(NVL(COEFF_DATA,0)+NVL(COEFF_ROAMING_DATA,0))/100 COEF_DATA,
                    SUM(NVL(COEF_SMS,0)+NVL(COEFF_ROAMING_SMS,0))/100 COEF_SMS
                FROM DIM.DT_CBM_REF_SOUSCRIPTION_PRICE 
                GROUP BY BDLE_NAME
            )B
            ON UPPER(TRIM(A.SUBSCRIPTION_SERVICE_DETAILS)) = UPPER(TRIM(B.BDLE_NAME))
            GROUP BY MSISDN

            UNION ALL

            select
                GET_NNP_MSISDN_9DIGITS(MSISDN) MSISDN, 
                SUM(0) REVENU_VOIX_PAYGO, 
                SUM(0) REVENU_DATA_PAYGO, 
                SUM(0) REVENU_SMS_PAYGO,  
                SUM(0) REVENU_VOIX_SUBS,
                SUM(0) REVENU_DATA_SUBS,
                SUM(0) REVENU_SMS_SUBS,
                SUM(nvl(AMOUNT, 0)) EMERGENCY_DATA,
                sum(0) revenu_vas_retail,
                sum(0) revenu_data_roaming,
                sum(0) revenu_credit_compte_desactive,
                SUM(0) revenu_p2p_voix,
                sum(0) revenu_p2p_data 
            from MON.SPARK_FT_EMERGENCY_DATA
            where TRANSACTION_DATE = '###SLICE_VALUE###' AND NVL(TRANSACTION_TYPE,'ND') ='LOAN'
            group by GET_NNP_MSISDN_9DIGITS(msisdn)

            UNION ALL

            select
                GET_NNP_MSISDN_9DIGITS(served_party_msisdn) MSISDN, 
                SUM(0) REVENU_VOIX_PAYGO, 
                SUM(0) REVENU_DATA_PAYGO, 
                SUM(0) REVENU_SMS_PAYGO,  
                SUM(0) REVENU_VOIX_SUBS,
                SUM(0) REVENU_DATA_SUBS,
                SUM(0) REVENU_SMS_SUBS,
                SUM(0) EMERGENCY_DATA,
                sum(nvl(MAIN_AMOUNT, 0)) revenu_vas_retail,
                sum(0) revenu_data_roaming,
                sum(0) revenu_credit_compte_desactive,
                SUM(0) revenu_p2p_voix,
                sum(0) revenu_p2p_data          
            from MON.SPARK_FT_SUBS_RETAIL_ZEBRA
            where TRANSACTION_DATE='###SLICE_VALUE###' AND MAIN_AMOUNT > 0
            group by GET_NNP_MSISDN_9DIGITS(served_party_msisdn)

            UNION ALL 

            select
                GET_NNP_MSISDN_9DIGITS(charged_party_msisdn) MSISDN, 
                SUM(0) REVENU_VOIX_PAYGO, 
                SUM(0) REVENU_DATA_PAYGO, 
                SUM(0) REVENU_SMS_PAYGO,  
                SUM(0) REVENU_VOIX_SUBS,
                SUM(0) REVENU_DATA_SUBS,
                SUM(0) REVENU_SMS_SUBS,
                SUM(0) EMERGENCY_DATA,
                sum(0) revenu_vas_retail,
                sum(nvl(MAIN_COST, 0)) revenu_data_roaming,
                sum(0) revenu_credit_compte_desactive,
                SUM(0) revenu_p2p_voix,
                sum(0) revenu_p2p_data             
            from mon.spark_ft_cra_gprs
            where session_date='###SLICE_VALUE###' and nvl(main_cost, 0)>=0 and roaming_indicator = '1'
            group by charged_party_msisdn

            UNION ALL

            select
                access_key msisdn,
                SUM(0) REVENU_VOIX_PAYGO, 
                SUM(0) REVENU_DATA_PAYGO, 
                SUM(0) REVENU_SMS_PAYGO,  
                SUM(0) REVENU_VOIX_SUBS,
                SUM(0) REVENU_DATA_SUBS,
                SUM(0) REVENU_SMS_SUBS,
                SUM(0) EMERGENCY_DATA,
                sum(0) revenu_vas_retail,
                sum(0) revenu_data_roaming,
                sum(nvl(MAIN_CREDIT, 0)) revenu_credit_compte_desactive,
                SUM(0) revenu_p2p_voix,
                sum(0) revenu_p2p_data
            from MON.SPARK_FT_CONTRACT_SNAPSHOT
            WHERE event_date='###SLICE_VALUE###' AND DEACTIVATION_DATE='###SLICE_VALUE###' AND MAIN_CREDIT > 0
            group by access_key

            UNION ALL 

            select
                GET_NNP_MSISDN_9DIGITS(sender_msisdn) msisdn,
                SUM(0) REVENU_VOIX_PAYGO, 
                SUM(0) REVENU_DATA_PAYGO, 
                SUM(0) REVENU_SMS_PAYGO,  
                SUM(0) REVENU_VOIX_SUBS,
                SUM(0) REVENU_DATA_SUBS,
                SUM(0) REVENU_SMS_SUBS,
                SUM(0) EMERGENCY_DATA,
                sum(0) revenu_vas_retail,
                sum(0) revenu_data_roaming,
                sum(0) revenu_credit_compte_desactive,
                SUM(nvl(TRANSFER_FEES, 0)) revenu_p2p_voix,
                sum(0) revenu_p2p_data
            from MON.SPARK_FT_CREDIT_TRANSFER
            WHERE REFILL_DATE='###SLICE_VALUE###'  AND TERMINATION_IND = '000'
            group by GET_NNP_MSISDN_9DIGITS(sender_msisdn)

            UNION ALL

            select
                GET_NNP_MSISDN_9DIGITS(SENDER_MSISDN) msisdn,
                SUM(0) REVENU_VOIX_PAYGO, 
                SUM(0) REVENU_DATA_PAYGO, 
                SUM(0) REVENU_SMS_PAYGO,  
                SUM(0) REVENU_VOIX_SUBS,
                SUM(0) REVENU_DATA_SUBS,
                SUM(0) REVENU_SMS_SUBS,
                SUM(0) EMERGENCY_DATA,
                sum(0) revenu_vas_retail,
                sum(0) revenu_data_roaming,
                sum(0) revenu_credit_compte_desactive,
                SUM(0) revenu_p2p_voix,
                sum(nvl(amount_charged, 0)) revenu_p2p_data
            from MON.SPARK_FT_DATA_TRANSFER A
            where TRANSACTION_DATE='###SLICE_VALUE###' AND amount_charged > 0
            group by GET_NNP_MSISDN_9DIGITS(SENDER_MSISDN)

        ) A000
        LEFT JOIN
        (
            SELECT
                NVL(F10.MSISDN, F11.MSISDN) MSISDN,
                UPPER(NVL(F11.SITE_NAME, F10.SITE_NAME)) SITE_NAME
            FROM
            (
                SELECT
                    MSISDN,
                    MAX(SITE_NAME) SITE_NAME
                FROM MON.SPARK_FT_CLIENT_LAST_SITE_DAY
                WHERE EVENT_DATE = '###SLICE_VALUE###'
                GROUP BY MSISDN
            ) F10
            FULL JOIN
            (
                SELECT
                    MSISDN,
                    MAX(SITE_NAME) SITE_NAME
                FROM MON.SPARK_FT_CLIENT_SITE_TRAFFIC_DAY
                WHERE EVENT_DATE = '###SLICE_VALUE###'
                GROUP BY MSISDN
            ) F11
            ON F10.MSISDN = F11.MSISDN
        ) A001
        ON A000.MSISDN=A001.MSISDN
        GROUP BY NVL(A000.MSISDN, A001.MSISDN)
    )
    GROUP BY SITE_NAME
) R
ON UPPER(TRIM(T.SITE_NAME)) = UPPER(TRIM(R.SITE_NAME))
FULL JOIN
(
    SELECT 
        LOC_SITE_NAME SITE_NAME,
        MAX(GROSS_ADD) GROSS_ADD
    FROM MON.SPARK_FT_SITE_360
    WHERE EVENT_DATE = '###SLICE_VALUE###'
    GROUP BY SITE_NAME
) A
ON UPPER(TRIM(T.SITE_NAME)) = UPPER(TRIM(A.SITE_NAME))
FULL JOIN
(
    SELECT
        SITE_NAME,
        MAX(KPI_VALUE) CALL_BOX
    FROM MON.SPARK_FT_GEOMARKETING_REPORT_360_MTD
    WHERE EVENT_DATE = '###SLICE_VALUE###' AND KPI_NAME='CALL_BOX'
    GROUP BY SITE_NAME
) E
ON UPPER(TRIM(T.SITE_NAME)) = UPPER(TRIM(E.SITE_NAME))
FULL JOIN
(
    SELECT
        SITE_NAME,
        MAX(KPI_VALUE) POS_OM
    FROM MON.SPARK_FT_GEOMARKETING_REPORT_360_MTD
    WHERE EVENT_DATE = '###SLICE_VALUE###' AND KPI_NAME='POS_OM'
    GROUP BY SITE_NAME
) F
ON UPPER(TRIM(T.SITE_NAME)) = UPPER(TRIM(F.SITE_NAME))
FULL JOIN
(
    SELECT
        SITE_NAME,
        MAX(KPI_VALUE) FAMOCO
    FROM MON.SPARK_FT_GEOMARKETING_REPORT_360_MTD
    WHERE EVENT_DATE = '###SLICE_VALUE###' AND KPI_NAME='FAMOCO'
    GROUP BY SITE_NAME
) F1
ON UPPER(TRIM(T.SITE_NAME)) = UPPER(TRIM(F1.SITE_NAME))
FULL JOIN
(
    SELECT
        SITE_NAME,
        MAX(KPI_VALUE) PARC_ART
    FROM MON.SPARK_FT_GEOMARKETING_REPORT_360
    WHERE EVENT_DATE = '###SLICE_VALUE###' AND KPI_NAME='PARC_ART'
    GROUP BY SITE_NAME
) G
ON UPPER(TRIM(T.SITE_NAME)) = UPPER(TRIM(G.SITE_NAME))
FULL JOIN
(
    select site_name,
        count(distinct aa.msisdn) parc_group
    from 
    (
        select
            msisdn,
            sum(1) parc_group
        from
        (
            select distinct
                access_key msisdn
            from MON.SPARK_FT_CONTRACT_SNAPSHOT
            where event_date=DATE_ADD('###SLICE_VALUE###',1) and CURRENT_STATUS in ('a', 'ACTIVE') and OSP_STATUS in ('a', 'ACTIVE')

            union

            select distinct
                msisdn
            from MON.SPARK_FT_ACCOUNT_ACTIVITY
            where event_date = DATE_ADD('###SLICE_VALUE###',1) and GP_STATUS='ACTIF'
        ) 
        group by msisdn
    )aa 
    left join 
    (   
        SELECT
            NVL(F10.MSISDN, F11.MSISDN) MSISDN,
            UPPER(NVL(F11.SITE_NAME, F10.SITE_NAME)) SITE_NAME
        FROM
        (
            SELECT
                MSISDN,
                MAX(SITE_NAME) SITE_NAME
            FROM MON.SPARK_FT_CLIENT_LAST_SITE_DAY
            WHERE EVENT_DATE = '###SLICE_VALUE###'
            GROUP BY MSISDN
        ) F10
        FULL JOIN
        (
            SELECT
                MSISDN,
                MAX(SITE_NAME) SITE_NAME
            FROM MON.SPARK_FT_CLIENT_SITE_TRAFFIC_DAY
            WHERE EVENT_DATE = '###SLICE_VALUE###'
            GROUP BY MSISDN
        ) F11
        ON F10.MSISDN = F11.MSISDN
    )bb on aa.msisdn = bb.msisdn
    where nvl(parc_group,0) > 0
    group by site_name
) H
ON UPPER(TRIM(T.SITE_NAME)) = UPPER(TRIM(H.SITE_NAME))
FULL JOIN
(
    select SITE_NAME,
        count(distinct A.msisdn) data_users
    from 
    (
        select distinct msisdn
        from 
        (
            select 
                CHARGED_PARTY_MSISDN msisdn,
                SUM(NVL(BYTES_RECEIVED,0)+ NVL(BYTES_SENT,0))/1024/1024 TRAFIC_DATA
            FROM MON.SPARK_FT_CRA_GPRS
            WHERE SESSION_DATE between concat(substring('###SLICE_VALUE###', 1, 7), '-01') and '###SLICE_VALUE###'  AND NVL(MAIN_COST, 0) >= 0
            group by CHARGED_PARTY_MSISDN
        ) 
        where trafic_data > 1
    ) A
    LEFT JOIN
    (
        SELECT
            NVL(F10.MSISDN, F11.MSISDN) MSISDN,
            UPPER(NVL(F11.SITE_NAME, F10.SITE_NAME)) SITE_NAME
        FROM
        (
            SELECT
                MSISDN,
                MAX(SITE_NAME) SITE_NAME
            FROM MON.SPARK_FT_CLIENT_LAST_SITE_DAY
            WHERE EVENT_DATE = '###SLICE_VALUE###'
            GROUP BY MSISDN
        ) F10
        FULL JOIN
        (
            SELECT
                MSISDN,
                MAX(SITE_NAME) SITE_NAME
            FROM MON.SPARK_FT_CLIENT_SITE_TRAFFIC_DAY
            WHERE EVENT_DATE = '###SLICE_VALUE###'
            GROUP BY MSISDN
        ) F11
        ON F10.MSISDN = F11.MSISDN
    )D ON A.msisdn = D.msisdn
    group by SITE_NAME
) P2
ON UPPER(TRIM(T.SITE_NAME)) = UPPER(TRIM(P2.SITE_NAME))
FULL JOIN
(
    SELECT
        SITE_NAME,
        MAX(KPI_VALUE) DATAUSERS
    FROM MON.SPARK_FT_GEOMARKETING_REPORT_360
    WHERE EVENT_DATE = '###SLICE_VALUE###' AND KPI_NAME='DATAUSERS'
    GROUP BY SITE_NAME
) J
ON UPPER(TRIM(T.SITE_NAME)) = UPPER(TRIM(J.SITE_NAME))
FULL JOIN
(
    SELECT 
        SITE_NAME, 
        COUNT (DISTINCT A.MSISDN) GROSS_ADD_DATA
    FROM 
    (
        SELECT
            MSISDN  
        FROM
        (
            SELECT 
                N.MSISDN,
                CASE WHEN NVL(DATA_USED, 0) > 1 THEN 1 ELSE 0 END TMP_GROSS_ADD_DATA
            FROM 
            (
                SELECT DISTINCT
                    SERVED_PARTY_MSISDN MSISDN
                FROM MON.SPARK_FT_SUBSCRIPTION
                WHERE TRANSACTION_DATE  between concat(substring('###SLICE_VALUE###', 1, 7), '-01') and '###SLICE_VALUE###' 
                AND SUBSCRIPTION_SERVICE LIKE '%PPS%' 
            ) N
            INNER JOIN 
            (
                SELECT 
                    SERVED_PARTY_MSISDN MSISDN,
                    (SUM(BYTES_SENT) + SUM(BYTES_RECEIVED)) /1024/1024 DATA_USED
                FROM MON.SPARK_FT_CRA_GPRS
                WHERE NVL(MAIN_COST, 0)>=0 and SESSION_DATE between concat(substring('###SLICE_VALUE###', 1, 7), '-01') and '###SLICE_VALUE###'
                GROUP BY SERVED_PARTY_MSISDN
            ) M
            ON N.MSISDN = M.MSISDN
        ) WHERE TMP_GROSS_ADD_DATA = 1 
    ) A
    LEFT JOIN 
    (
        SELECT
            NVL(F10.MSISDN, F11.MSISDN) MSISDN,
            UPPER(NVL(F11.SITE_NAME, F10.SITE_NAME)) SITE_NAME
        FROM
        (
            SELECT
                MSISDN,
                MAX(SITE_NAME) SITE_NAME
            FROM MON.SPARK_FT_CLIENT_LAST_SITE_DAY
            WHERE EVENT_DATE = '###SLICE_VALUE###'
            GROUP BY MSISDN
        ) F10
        FULL JOIN
        (
            SELECT
                MSISDN,
                MAX(SITE_NAME) SITE_NAME
            FROM MON.SPARK_FT_CLIENT_SITE_TRAFFIC_DAY
            WHERE EVENT_DATE = '###SLICE_VALUE###'
            GROUP BY MSISDN
        ) F11
        ON F10.MSISDN = F11.MSISDN
    ) L
    ON A.MSISDN = L.MSISDN
    GROUP BY SITE_NAME  
) O
ON UPPER(TRIM(T.SITE_NAME)) = UPPER(TRIM(O.SITE_NAME))
FULL JOIN
(
    SELECT
        SITE_NAME,
        COUNT(DISTINCT G.MSISDN) CHARGED_BASE
    FROM
    (
        SELECT 
            MSISDN
        FROM
        (
            SELECT 
                MSISDN,
                MAX(CASE WHEN OG_CALL >= DATE_SUB(EVENT_DATE,31) OR LEAST(IC_CALL_4,IC_CALL_3,IC_CALL_2,IC_CALL_1) >= DATE_SUB(EVENT_DATE,31) THEN 1 ELSE 0 END) CHARGED 
            FROM MON.SPARK_FT_ACCOUNT_ACTIVITY A
            WHERE EVENT_DATE = DATE_ADD('###SLICE_VALUE###', 1)
            GROUP BY MSISDN  
        ) WHERE CHARGED = 1 
    ) G
    LEFT JOIN 
    (
        SELECT
            NVL(F10.MSISDN, F11.MSISDN) MSISDN,
            UPPER(NVL(F11.SITE_NAME, F10.SITE_NAME)) SITE_NAME
        FROM
        (
            SELECT
                MSISDN,
                MAX(SITE_NAME) SITE_NAME
            FROM MON.SPARK_FT_CLIENT_LAST_SITE_DAY
            WHERE EVENT_DATE = '###SLICE_VALUE###'
            GROUP BY MSISDN
        ) F10
        FULL JOIN
        (
            SELECT
                MSISDN,
                MAX(SITE_NAME) SITE_NAME
            FROM MON.SPARK_FT_CLIENT_SITE_TRAFFIC_DAY
            WHERE EVENT_DATE = '###SLICE_VALUE###'
            GROUP BY MSISDN
        ) F11
        ON F10.MSISDN = F11.MSISDN
    ) L
    ON G.MSISDN = L.MSISDN
    GROUP BY SITE_NAME
) P
ON UPPER(TRIM(T.SITE_NAME)) = UPPER(TRIM(P.SITE_NAME))
FULL JOIN
(
    SELECT
        SITE_NAME,
        COUNT(A.MSISDN) GROSS_ADD_OM
    FROM
    (
        SELECT 
            MSISDN
        FROM 
        (
            SELECT 
                N.MSISDN,
                CASE WHEN M.MSISDN IS NOT NULL THEN 1 ELSE 0 END GAOM
            FROM 
            (
                SELECT DISTINCT
                    SERVED_PARTY_MSISDN MSISDN
                    FROM MON.SPARK_FT_SUBSCRIPTION
                WHERE TRANSACTION_DATE  between concat(substring('###SLICE_VALUE###', 1, 7), '-01') and '###SLICE_VALUE###'
                AND SUBSCRIPTION_SERVICE LIKE '%PPS%' 
            ) N
            INNER JOIN 
            (
                SELECT DISTINCT
                    SENDER_MSISDN MSISDN
                FROM CDR.SPARK_IT_OMNY_TRANSACTIONS
                WHERE TRANSFER_DATETIME  between concat(substring('###SLICE_VALUE###', 1, 7), '-01') and '###SLICE_VALUE###'

                UNION

                SELECT DISTINCT
                    RECEIVER_MSISDN MSISDN
                FROM CDR.SPARK_IT_OMNY_TRANSACTIONS
                WHERE TRANSFER_DATETIME  between concat(substring('###SLICE_VALUE###', 1, 7), '-01') and '###SLICE_VALUE###'
            ) M
            ON N.MSISDN = M.MSISDN
        ) WHERE GAOM = 1
    ) A
    LEFT JOIN 
    (
        SELECT
            NVL(F10.MSISDN, F11.MSISDN) MSISDN,
            UPPER(NVL(F11.SITE_NAME, F10.SITE_NAME)) SITE_NAME
        FROM
        (
            SELECT
                MSISDN,
                MAX(SITE_NAME) SITE_NAME
            FROM MON.SPARK_FT_CLIENT_LAST_SITE_DAY
            WHERE EVENT_DATE = '###SLICE_VALUE###'
            GROUP BY MSISDN
        ) F10
        FULL JOIN
        (
            SELECT
                MSISDN,
                MAX(SITE_NAME) SITE_NAME
            FROM MON.SPARK_FT_CLIENT_SITE_TRAFFIC_DAY
            WHERE EVENT_DATE = '###SLICE_VALUE###'
            GROUP BY MSISDN
        ) F11
        ON F10.MSISDN = F11.MSISDN
    ) L
    ON A.MSISDN = L.MSISDN
    GROUP BY SITE_NAME
) Q
ON UPPER(TRIM(T.SITE_NAME)) = UPPER(TRIM(Q.SITE_NAME))