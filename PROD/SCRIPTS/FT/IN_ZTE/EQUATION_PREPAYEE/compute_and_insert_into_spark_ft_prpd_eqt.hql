INSERT INTO MON.SPARK_FT_PRPD_EQT PARTITION(EVENT_DAY)
SELECT
    ACCT_ID,
    MSISDN,
    MAIN_DEBUT,
    MAIN_FIN,
    MAIN_DEBIT,
    MAIN_CREDIT,
    LOAN_DEBUT,
    LOAN_FIN,
    CASE WHEN CAST(LOAN_DEBUT+LOAN_CREDIT-LOAN_DEBIT - LOAN_FIN AS DECIMAL(13,2)) > 0
            AND   LOAN_EXP_DATE <= EVENT_DAY  AND LOAN_FIN = 0  THEN LOAN_DEBUT+LOAN_CREDIT-LOAN_FIN
         ELSE LOAN_DEBIT
    END LOAN_DEBIT,
    CASE WHEN CAST(LOAN_DEBUT+LOAN_CREDIT-LOAN_DEBIT - LOAN_FIN AS DECIMAL(13,2)) < 0
            AND   LOAN_EXP_DATE <= EVENT_DAY  AND LOAN_FIN = 0  THEN LOAN_DEBUT+LOAN_CREDIT-LOAN_FIN
         ELSE LOAN_CREDIT
    END LOAN_CREDIT,
    SASSAYE_DEBUT,
    SASSAYE_FIN,
    SASSAYE_DEBIT,
    SASSAYE_CREDIT,
    INSERT_DATE,
    STATUS,
    ACCOUNT_TYPE,
    CASE WHEN CAST(LOAN_DEBUT+LOAN_CREDIT-LOAN_DEBIT - LOAN_FIN AS DECIMAL(13,2))!= 0
            AND   LOAN_EXP_DATE <= EVENT_DAY  AND LOAN_FIN = 0  THEN CONCAT_WS('|','CHURNING',CAST(CAST(LOAN_DEBUT+LOAN_CREDIT-LOAN_DEBIT - LOAN_FIN AS DECIMAL(13,2)) AS STRING),COMMENTS)
         ELSE COMMENTS
    END COMMENTS,
    MAIN_EXP_DATE,
    LOAN_EXP_DATE,
    SASSAYE_EXP_DATE,
    ADJUSTMENT,
    MAIN_CREDIT_R,
    MAIN_DEBIT_R,
    LOAN_CREDIT_R,
    LOAN_DEBIT_R,
    SASSAYE_CREDIT_R,
    SASSAYE_DEBIT_R,
    UPDATE_DATE,
    EVENT_DAY
FROM (
    SELECT
        '###SLICE_VALUE###' EVENT_DAY,
        a.ACCT_ID,
        a.MSISDN,
        nvl(MAIN_DEBUT, 0) MAIN_DEBUT,
        nvl(MAIN_FIN, 0) MAIN_FIN,
        (case when main_fin is null and main_debut is not null then nvl(main_debut,0)+nvl(MAIN_credit, 0)-nvl(MAIN_DEBIT, 0)
             else 0
        end) + nvl(MAIN_DEBIT, 0) MAIN_DEBIT,
        nvl(MAIN_DEBIT, 0) main_debit_R,
        (case when main_debut is null and main_fin is not null then nvl(main_fin,0)-nvl(MAIN_credit, 0)+nvl(MAIN_DEBIT, 0)
                else 0
        end) + nvl(MAIN_CREDIT, 0) MAIN_CREDIT,
        nvl(MAIN_CREDIT, 0) MAIN_CREDIT_R,
        nvl(LOAN_DEBUT, 0) LOAN_DEBUT,
        nvl(LOAN_FIN, 0) LOAN_FIN,
        (case when loan_fin is null and loan_debut is not null then nvl(loan_debut,0)+nvl(loan_credit, 0)-nvl(loan_DEBIT, 0)
             else 0
        end) + nvl(LOAN_DEBIT, 0) LOAN_DEBIT,
        nvl(LOAN_DEBIT, 0) LOAN_DEBIT_R,
        (case when loan_debut is null and loan_fin is not null then nvl(loan_fin,0)-nvl(loan_credit, 0)+nvl(loan_DEBIT, 0)
                else 0
        end) + nvl(LOAN_CREDIT, 0) LOAN_CREDIT,
        nvl(LOAN_CREDIT, 0) LOAN_CREDIT_R,
        nvl(SASSAYE_DEBUT, 0) SASSAYE_DEBUT,
        nvl(SASSAYE_FIN, 0) SASSAYE_FIN,
        (case when sassaye_fin is null and sassaye_debut is not null then nvl(sassaye_debut,0)+nvl(sassaye_credit, 0)-nvl(sassaye_DEBIT, 0)
             else 0
        end) + nvl(SASSAYE_DEBIT, 0) SASSAYE_DEBIT,
        nvl(SASSAYE_DEBIT, 0) SASSAYE_DEBIT_R,
        (case when sassaye_debut is null and sassaye_fin is not null then nvl(sassaye_fin,0)-nvl(sassaye_credit, 0)+nvl(sassaye_DEBIT, 0)
                else 0
        end) + nvl(SASSAYE_CREDIT, 0) SASSAYE_CREDIT,
        nvl(SASSAYE_CREDIT, 0) SASSAYE_CREDIT_R,
        CURRENT_TIMESTAMP INSERT_DATE,
        state status,
        account_type,
        (case when main_fin is null and main_debut is not null then CONCAT_WS('|','MAIN',cast(nvl(main_debut,0)+nvl(MAIN_credit, 0)-nvl(MAIN_DEBIT, 0) as string),
                'LOAN', cast(nvl(LOAN_debut,0)+nvl(LOAN_credit, 0)-nvl(LOAN_DEBIT, 0) as string),
                'SASSAYE',cast( nvl(SASSAYE_debut,0)+nvl(SASSAYE_credit, 0)-nvl(SASSAYE_DEBIT, 0) as string))
            when main_debut is null and main_fin is not null then CONCAT_WS('|','MAIN',cast(nvl(main_fin,0)-nvl(MAIN_credit, 0)+nvl(MAIN_DEBIT, 0) as string)
                ,'LOAN', cast(nvl(LOAN_fin,0)-nvl(LOAN_credit, 0)+nvl(LOAN_DEBIT, 0) as string)
                ,'SASSAYE', cast(nvl(SASSAYE_fin,0)-nvl(SASSAYE_credit, 0)+nvl(SASSAYE_DEBIT, 0) as string))
            else NULL
        end)adjustment,
        CONCAT_WS('|',(case when main_fin is null and main_debut is not null then 'End computed'
            when main_debut is null and main_fin is not null then 'Begin computed'
            when main_fin is null and main_debut is null then 'Begin end computed'
            else 'No computation'
        end),comments) comments,
        NVL(c.main_exp_date, b.main_exp_date) main_exp_date,
        NVL(c.loan_exp_date, b.loan_exp_date) loan_exp_date,
        NVL(c.sassaye_exp_date, b.sassaye_exp_date) sassaye_exp_date,
        a.update_date
    FROM (
        SELECT
            DISTINCT
            a.ACCT_ID,
            subs.ACC_NBR MSISDN,
            subs.UPDATE_DATE,
            (CASE WHEN PROD.PROD_STATE='G' THEN 'VALID'
               WHEN PROD.PROD_STATE='A' THEN 'ACTIVE'
               WHEN (PROD.PROD_STATE='D' OR (PROD.PROD_STATE='E' AND PROD.BLOCK_REASON='20000000000000'))THEN 'INACTIVE'
               WHEN (PROD.PROD_STATE='E'AND PROD.BLOCK_REASON <> '20000000000000') THEN 'DEACTIVATED'
               WHEN PROD.PROD_STATE='B' THEN 'TERMINATED'
               ELSE PROD.PROD_STATE END
            ) state,
            g.account_type
        FROM ( SELECT bal.*, row_number() over (partition by bal.acct_id, bal.acct_res_id order by bal.update_date desc) rn FROM CDR.SPARK_IT_ZTE_BAL_SNAP bal WHERE bal.ORIGINAL_FILE_DATE = '###SLICE_VALUE###') a
        LEFT JOIN CDR.SPARK_IT_ZTE_SUBS_EXTRACT subs ON a.acct_id= subs.acct_id AND SUBS.ORIGINAL_FILE_DATE =DATE_SUB('###SLICE_VALUE###',-1)
        LEFT JOIN CDR.SPARK_IT_ZTE_prod_EXTRACT PROD ON subs.subs_id = prod.prod_id AND PROD.ORIGINAL_FILE_DATE =DATE_SUB('###SLICE_VALUE###',-1)
        LEFT JOIN (
            SELECT DISTINCT PRICE_PLAN_ID,
                (CASE WHEN UPPER(PRICE_PLAN_NAME) LIKE 'POSTPAID%' THEN 'POSTPAID'
                       WHEN UPPER(PRICE_PLAN_NAME) LIKE 'PREPAID%' THEN 'PREPAID'
                  ELSE 'PREPAID' END ) ACCOUNT_TYPE
            FROM CDR.SPARK_IT_ZTE_PRICE_PLAN_EXTRACT PRICE_PLAN
            WHERE PRICE_PLAN.ORIGINAL_FILE_DATE=DATE_SUB('###SLICE_VALUE###',-1)
        ) g ON subs.PRICE_PLAN_ID= g.PRICE_PLAN_ID
        WHERE RN=1
    ) a
    LEFT JOIN (
        SELECT b.*, ACC_NBR MSISDN , 'ACCT_ID_OK' COMMENTS, row_number() OVER (PARTITION BY acc_nbr ORDER BY UPDATE_DATE DESC, subs_id desc) rn
        FROM cdr.spark_IT_ZTE_SUBS_EXTRACT b WHERE b.ORIGINAL_FILE_DATE=DATE_SUB('###SLICE_VALUE###',-1)
    ) ab ON a.acct_id = ab.acct_id AND RN=1
    LEFT JOIN (
        SELECT ACCT_ID
            ,SUM(CASE WHEN BAL.ACCT_RES_ID = 1 THEN -GROSS_BAL/100-CONSUME_BAL/100-RESERVE_BAL/100 ELSE 0 END) MAIN_debut
            ,SUM(CASE WHEN BAL.ACCT_RES_ID = 20 THEN -GROSS_BAL/100-CONSUME_BAL/100-RESERVE_BAL/100 ELSE 0 END) LOAN_debut
            ,SUM(CASE WHEN BAL.ACCT_RES_ID = 21 THEN -GROSS_BAL/100-CONSUME_BAL/100-RESERVE_BAL/100 ELSE 0 END) SASSAYE_debut
            ,max(CASE WHEN BAL.ACCT_RES_ID = 1 THEN exp_date ELSE null END ) MAIN_exp_date
            ,max(CASE WHEN BAL.ACCT_RES_ID = 20 THEN exp_date ELSE null END) LOAN_exp_date
            ,max(CASE WHEN BAL.ACCT_RES_ID = 21 THEN exp_date ELSE null END) SASSAYE_exp_date
        FROM CDR.SPARK_IT_ZTE_BAL_SNAP bal
        WHERE bal.ORIGINAL_FILE_DATE = DATE_SUB('###SLICE_VALUE###',1)
        GROUP BY ACCT_ID
    ) b ON a.acct_id=b.acct_id
    LEFT JOIN (
        SELECT ACCT_ID
            ,SUM(CASE WHEN BAL.ACCT_RES_ID = 1 THEN -GROSS_BAL/100-CONSUME_BAL/100-RESERVE_BAL/100 ELSE 0 END) MAIN_FIN
            ,SUM(CASE WHEN BAL.ACCT_RES_ID = 20 THEN -GROSS_BAL/100-CONSUME_BAL/100-RESERVE_BAL/100 ELSE 0 END) LOAN_FIN
            ,SUM(CASE WHEN BAL.ACCT_RES_ID = 21 THEN -GROSS_BAL/100-CONSUME_BAL/100-RESERVE_BAL/100 ELSE 0 END) SASSAYE_FIN
            ,max(CASE WHEN BAL.ACCT_RES_ID = 1 THEN exp_date ELSE null END) MAIN_exp_date
            ,max(CASE WHEN BAL.ACCT_RES_ID = 20 THEN exp_date ELSE null END) LOAN_exp_date
            ,max(CASE WHEN BAL.ACCT_RES_ID = 21 THEN exp_date ELSE null END) SASSAYE_exp_date
        FROM CDR.SPARK_IT_ZTE_BAL_SNAP bal BAL
        WHERE bal.ORIGINAL_FILE_DATE = '###SLICE_VALUE###'
        GROUP BY ACCT_ID
    ) c ON c.acct_id=a.acct_id
    LEFT JOIN AGG.SPARK_FT_A_EDR_PRPD_EQT e ON e.acct_id_msisdn = ab.msisdn AND RN=1 AND e.event_day = '###SLICE_VALUE###'
)T
