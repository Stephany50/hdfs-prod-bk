INSERT INTO tmp.TT_TMP_BALANCE_AGEE_1
SELECT custcode AS code_client, prgcode AS categorie,
        IF(TRIM(ccname)='', IF(TRIM(ccline2)='', TRIM(ccline3), TRIM(ccline2)), TRIM(ccname)) as nom, --DECODE(TRIM(ccname), '', DECODE(TRIM(ccline2), '', TRIM(ccline3), TRIM(ccline2)), TRIM(ccname)) AS nom,
        SUM(balance_current) AS balance,
        MAX(bill_date) AS derniere_facture,
        SUM(CASE WHEN ohentdate >= ADD_MONTHS ('2019-11-24', 0) THEN balance_prev_period ELSE 0 END) AS balance_J,
        SUM(CASE WHEN ohentdate >= ADD_MONTHS ('2019-11-24', -1) AND ohentdate < ADD_MONTHS ('2019-11-24', 0) THEN balance_prev_period ELSE 0 END) AS balance_J_30,
        SUM(CASE WHEN ohentdate >= ADD_MONTHS ('2019-11-24', -2) AND ohentdate < ADD_MONTHS ('2019-11-24', -1) THEN balance_prev_period ELSE 0 END) AS balance_J_60,
        SUM(CASE WHEN ohentdate >= ADD_MONTHS ('2019-11-24', -3) AND ohentdate < ADD_MONTHS ('2019-11-24', -2) THEN balance_prev_period ELSE 0 END) AS balance_J_90,
        SUM(CASE WHEN ohentdate >= ADD_MONTHS ('2019-11-24', -4) AND ohentdate < ADD_MONTHS ('2019-11-24', -3) THEN balance_prev_period ELSE 0 END) AS balance_J_120,
        SUM(CASE WHEN ohentdate >= ADD_MONTHS ('2019-11-24', -5) AND ohentdate < ADD_MONTHS ('2019-11-24', -4) THEN balance_prev_period ELSE 0 END) AS balance_J_150,
        SUM(CASE WHEN ohentdate >= ADD_MONTHS ('2019-11-24', -6) AND ohentdate < ADD_MONTHS ('2019-11-24', -5) THEN balance_prev_period ELSE 0 END) AS balance_J_180,
        SUM(CASE WHEN ohentdate >= ADD_MONTHS ('2019-11-24', -12) AND ohentdate < ADD_MONTHS ('2019-11-24', -6) THEN balance_prev_period ELSE 0 END) AS balance_J_360,
        SUM(CASE WHEN ohentdate >= ADD_MONTHS ('2019-11-24', -24) AND ohentdate < ADD_MONTHS ('2019-11-24', -12) THEN balance_prev_period ELSE 0 END) AS balance_J_720,
        SUM(CASE WHEN ohentdate < ADD_MONTHS ('2019-11-24', -24) THEN balance_prev_period ELSE 0 END) AS balance_J_720_Plus,
        billcycle AS BILLCYCLE_CODE, a.customer_id, '2019-11-24' AS DATE_PERIODE_REF, current_timestamp AS INSERT_DATE , '2019-11-24' event_date -- ajout snr
    FROM tmp.TMP_CUST_CONTACT_ORDER a
    GROUP BY custcode, prgcode, IF(TRIM(ccname)='', IF(TRIM(ccline2)='', TRIM(ccline3), TRIM(ccline2)), TRIM(ccname)),
        billcycle, a.customer_id