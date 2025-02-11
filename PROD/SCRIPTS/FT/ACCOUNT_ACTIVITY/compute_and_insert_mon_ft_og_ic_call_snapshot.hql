INSERT INTO MON.FT_OG_IC_CALL_SNAPSHOT PARTITION(EVENT_DATE)

   SELECT 

    fn_format_msisdn_to_9digits(NVL(A.MSISDN,B.MSISDN)) MSISDN,

    NVL(A.OG_CALL,B.OG_CALL) OG_CALL,

    NVL(A.IC_CALL_1,B.IC_CALL_1) IC_CALL_1,

    NVL(A.IC_CALL_2,B.IC_CALL_2) IC_CALL_2,

    NVL(A.IC_CALL_3,B.IC_CALL_3) IC_CALL_3,

    NVL(A.IC_CALL_4,B.IC_CALL_4) IC_CALL_4,

    current_timestamp INSERT_DATE,

    '###SLICE_VALUE###' EVENT_DATE

FROM

    (SELECT * FROM MON.FT_OG_IC_CALL_SNAPSHOT WHERE EVENT_DATE = DATE_SUB('###SLICE_VALUE###',1)) A

    FULL OUTER JOIN

    MON.TMP_OG_IC_CALL_SNAPSHOT_PRE B

    ON (A.MSISDN = B.MSISDN)

WHERE

    A.MSISDN iS NULL or B.MSISDN IS NULL

UNION ALL

SELECT

    fn_format_msisdn_to_9digits(D.MSISDN)MSISDN ,

    TO_DATE(fn_get_sorted_list_item (CONCAT_WS('|',CAST(TO_DATE(D.OG_CALL) AS string), CAST(TO_DATE(S.OG_CALL) AS string)), 1  , 'DESC','\\|'))  OG_CALL,

    TO_DATE(fn_get_sorted_list_item (CONCAT_WS('|',CAST(TO_DATE(D.IC_CALL_1) AS string),CAST(TO_DATE(S.IC_CALL_1) AS string), CAST(TO_DATE(D.IC_CALL_2) AS string), CAST(TO_DATE(S.IC_CALL_2) AS string), CAST(TO_DATE(D.IC_CALL_3) AS string), CAST(TO_DATE(S.IC_CALL_3) AS string), CAST(TO_DATE(D.IC_CALL_4) AS string), CAST(TO_DATE(S.IC_CALL_4) AS string)), 4, 'DESC','\\|'))  IC_CALL_1,

    TO_DATE(fn_get_sorted_list_item (CONCAT_WS('|',CAST(TO_DATE(D.IC_CALL_1) AS string), CAST(TO_DATE(S.IC_CALL_1) AS string), CAST(TO_DATE(D.IC_CALL_2) AS string), CAST(TO_DATE(S.IC_CALL_2) AS string), CAST(TO_DATE(D.IC_CALL_3) AS string), CAST(TO_DATE(S.IC_CALL_3) AS string), CAST(TO_DATE(D.IC_CALL_4) AS string), CAST(TO_DATE(S.IC_CALL_4) AS string)), 3, 'DESC','\\|'))  IC_CALL_2,

    TO_DATE(fn_get_sorted_list_item (CONCAT_WS('|',CAST(TO_DATE(D.IC_CALL_1) AS string), CAST(TO_DATE(S.IC_CALL_1) AS string), CAST(TO_DATE(D.IC_CALL_2) AS string), CAST(TO_DATE(S.IC_CALL_2) AS string), CAST(TO_DATE(D.IC_CALL_3) AS string), CAST(TO_DATE(S.IC_CALL_3) AS string), CAST(TO_DATE(D.IC_CALL_4) AS string), CAST(TO_DATE(S.IC_CALL_4) AS string)), 2, 'DESC','\\|'))  IC_CALL_3,

    TO_DATE(fn_get_sorted_list_item (CONCAT_WS('|',CAST(TO_DATE(D.IC_CALL_1) AS string), CAST(TO_DATE(S.IC_CALL_1) AS string), CAST(TO_DATE(D.IC_CALL_2) AS string), CAST(TO_DATE(S.IC_CALL_2) AS string), CAST(TO_DATE(D.IC_CALL_3) AS string), CAST(TO_DATE(S.IC_CALL_3) AS string), CAST(TO_DATE(D.IC_CALL_4) AS string), CAST(TO_DATE(S.IC_CALL_4) AS string)), 1, 'DESC','\\|'))  IC_CALL_4,

    current_timestamp INSERT_DATE,

    '###SLICE_VALUE###'  EVENT_DATE

FROM

    MON.FT_OG_IC_CALL_SNAPSHOT D,

    mon.TMP_OG_IC_CALL_SNAPSHOT_PRE S

WHERE

    D.EVENT_DATE = (DATE_SUB('###SLICE_VALUE###',1))

AND fn_format_msisdn_to_9digits(D.MSISDN) = S.MSISDN
