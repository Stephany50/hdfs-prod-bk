INSERT INTO TMP.TT_TMP_BALANCE_AGEE
SELECT a.EVENT_DATE, CUSTCODE AS CODE_CLIENT, PRGCODE AS CATEGORIE,
        IF(TRIM(ccname)='', IF(TRIM(ccline2)='', TRIM(ccline3), TRIM(ccline2)), TRIM(ccname)) as NOM,
        SUM(nvl(balance,0.00)) AS balance, MAX(bill_date) AS derniere_facture,
        SUM(CASE WHEN ohentdate >= ADD_MONTHS (EVENT_DATE, 0) THEN balance_prev_period ELSE 0 END) AS balance_J,
        SUM(CASE WHEN ohentdate >= ADD_MONTHS (EVENT_DATE, -1) AND ohentdate < ADD_MONTHS (EVENT_DATE, 0) THEN balance_prev_period ELSE 0 END) AS balance_J_30,
        SUM(CASE WHEN ohentdate >= ADD_MONTHS (EVENT_DATE, -2) AND ohentdate < ADD_MONTHS (EVENT_DATE, -1) THEN balance_prev_period ELSE 0 END) AS balance_J_60,
        SUM(CASE WHEN ohentdate >= ADD_MONTHS (EVENT_DATE, -3) AND ohentdate < ADD_MONTHS (EVENT_DATE, -2) THEN balance_prev_period ELSE 0 END) AS balance_J_90,
        SUM(CASE WHEN ohentdate >= ADD_MONTHS (EVENT_DATE, -4) AND ohentdate < ADD_MONTHS (EVENT_DATE, -3) THEN balance_prev_period ELSE 0 END) AS balance_J_120,
        SUM(CASE WHEN ohentdate >= ADD_MONTHS (EVENT_DATE, -5) AND ohentdate < ADD_MONTHS (EVENT_DATE, -4) THEN balance_prev_period ELSE 0 END) AS balance_J_150,
        SUM(CASE WHEN ohentdate >= ADD_MONTHS (EVENT_DATE, -6) AND ohentdate < ADD_MONTHS (EVENT_DATE, -5) THEN balance_prev_period ELSE 0 END) AS balance_J_180,
        SUM(CASE WHEN ohentdate >= ADD_MONTHS (EVENT_DATE, -12) AND ohentdate < ADD_MONTHS (EVENT_DATE, -6) THEN balance_prev_period ELSE 0 END) AS balance_J_360,
        SUM(CASE WHEN ohentdate >= ADD_MONTHS (EVENT_DATE, -24) AND ohentdate < ADD_MONTHS (EVENT_DATE, -12) THEN balance_prev_period ELSE 0 END) AS balance_J_720,
        SUM(CASE WHEN ohentdate < ADD_MONTHS (EVENT_DATE, -24) THEN balance_prev_period ELSE 0 END) AS balance_J_720_Plus,
        billcycle AS BILLCYCLE_CODE, a.CUSTOMER_ID, max(balance_current) balance_current, count(distinct ohrefnum) nbre_facture, count(*) nbre_entree, current_timestamp AS INSERT_DATE
FROM TMP.TMP_CUST_CONTACT_ORDER A
WHERE EVENT_DATE = '###SLICE_VALUE###'
GROUP BY EVENT_DATE, CUSTCODE, PRGCODE, IF(TRIM(CCNAME)='', IF(TRIM(CCLINE2)='', TRIM(CCLINE3), TRIM(CCLINE2)), TRIM(CCNAME)), BILLCYCLE, A.CUSTOMER_ID
