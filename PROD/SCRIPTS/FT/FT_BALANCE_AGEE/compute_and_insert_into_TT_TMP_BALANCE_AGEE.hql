insert into tmp.TT_TMP_BALANCE_AGEE_2
SELECT custcode AS code_client, prgcode AS categorie,
        IF(TRIM(ccname)='', IF(TRIM(ccline2)='', TRIM(ccline3), TRIM(ccline2)), TRIM(ccname)) as nom,
        SUM(nvl(balance_current,0)) AS balance, MAX(bill_date) AS derniere_facture,
        SUM(CASE WHEN ohentdate >= ADD_MONTHS ('###SLICE_VALUE###', 0) THEN balance_prev_period ELSE 0 END) AS balance_J,
        SUM(CASE WHEN ohentdate >= ADD_MONTHS ('###SLICE_VALUE###', -1) AND ohentdate < ADD_MONTHS ('###SLICE_VALUE###', 0) THEN balance_prev_period ELSE 0 END) AS balance_J_30,
        SUM(CASE WHEN ohentdate >= ADD_MONTHS ('###SLICE_VALUE###', -2) AND ohentdate < ADD_MONTHS ('###SLICE_VALUE###', -1) THEN balance_prev_period ELSE 0 END) AS balance_J_60,
        SUM(CASE WHEN ohentdate >= ADD_MONTHS ('###SLICE_VALUE###', -3) AND ohentdate < ADD_MONTHS ('###SLICE_VALUE###', -2) THEN balance_prev_period ELSE 0 END) AS balance_J_90,
        SUM(CASE WHEN ohentdate >= ADD_MONTHS ('###SLICE_VALUE###', -4) AND ohentdate < ADD_MONTHS ('###SLICE_VALUE###', -3) THEN balance_prev_period ELSE 0 END) AS balance_J_120,
        SUM(CASE WHEN ohentdate >= ADD_MONTHS ('###SLICE_VALUE###', -5) AND ohentdate < ADD_MONTHS ('###SLICE_VALUE###', -4) THEN balance_prev_period ELSE 0 END) AS balance_J_150,
        SUM(CASE WHEN ohentdate >= ADD_MONTHS ('###SLICE_VALUE###', -6) AND ohentdate < ADD_MONTHS ('###SLICE_VALUE###', -5) THEN balance_prev_period ELSE 0 END) AS balance_J_180,
        SUM(CASE WHEN ohentdate >= ADD_MONTHS ('###SLICE_VALUE###', -12) AND ohentdate < ADD_MONTHS ('###SLICE_VALUE###', -6) THEN balance_prev_period ELSE 0 END) AS balance_J_360,
        SUM(CASE WHEN ohentdate >= ADD_MONTHS ('###SLICE_VALUE###', -24) AND ohentdate < ADD_MONTHS ('###SLICE_VALUE###', -12) THEN balance_prev_period ELSE 0 END) AS balance_J_720,
        SUM(CASE WHEN ohentdate < ADD_MONTHS ('###SLICE_VALUE###', -24) THEN balance_prev_period ELSE 0 END) AS balance_J_720_Plus,
        billcycle AS BILLCYCLE_CODE, a.customer_id, a.event_date AS DATE_PERIODE_REF, current_timestamp AS INSERT_DATE , a.event_date event_date -- ajout snr
FROM tmp.TMP_CUST_CONTACT_ORDER_2 a
where event_date = '###SLICE_VALUE###'
GROUP BY event_date, custcode, prgcode, IF(TRIM(ccname)='', IF(TRIM(ccline2)='', TRIM(ccline3), TRIM(ccline2)), TRIM(ccname)),
    billcycle, a.customer_id;