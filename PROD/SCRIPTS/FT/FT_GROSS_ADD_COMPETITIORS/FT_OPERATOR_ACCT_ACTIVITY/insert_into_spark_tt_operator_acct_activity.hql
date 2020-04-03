
INSERT INTO MON.SPARK_TT_OPERATOR_ACCT_ACTIVITY

SELECT

     nvl(a.MSISDN, b.MSISDN) MSISDN,
      IF(a.msisdn IS NULL OR b.msisdn IS NULL  , nvl(a.OG_CALL_1, b.OG_CALL_1),TO_DATE ( mon.fn_get_sorted_list_item ( TO_CHAR( a.OG_CALL_1,'yyyymmdd')||'|'||TO_CHAR( a.OG_CALL_2,'yyyymmdd')||'|'|| TO_CHAR( a.OG_CALL_3,'yyyymmdd')||'|'||TO_CHAR( a.OG_CALL_4,'yyyymmdd')||'|'||TO_CHAR( b.OG_CALL_1,'yyyymmdd')||'|'||TO_CHAR( b.OG_CALL_2,'yyyymmdd')||'|'|| TO_CHAR( b.OG_CALL_3,'yyyymmdd')||'|'||TO_CHAR( b.OG_CALL_4,'yyyymmdd'), 4 , 'DESC', '|', ':'), 'yyyymmdd')
        ) OG_CALL_1,
    IF(a.msisdn IS NULL OR b.msisdn IS NULL , nvl(a.OG_CALL_2, b.OG_CALL_2),TO_DATE ( mon.fn_get_sorted_list_item ( TO_CHAR( a.OG_CALL_1,'yyyymmdd')||'|'||TO_CHAR( a.OG_CALL_2,'yyyymmdd')||'|'|| TO_CHAR( a.OG_CALL_3,'yyyymmdd')||'|'||TO_CHAR( a.OG_CALL_4,'yyyymmdd')||'|'||TO_CHAR( b.OG_CALL_1,'yyyymmdd')||'|'||TO_CHAR( b.OG_CALL_2,'yyyymmdd')||'|'|| TO_CHAR( b.OG_CALL_3,'yyyymmdd')||'|'||TO_CHAR( b.OG_CALL_4,'yyyymmdd'), 3 , 'DESC', '|', ':'), 'yyyymmdd') ) OG_CALL_2,
    IF(a.msisdn IS NULL OR b.msisdn IS NULL ,nvl(a.OG_CALL_3, b.OG_CALL_3),TO_DATE ( mon.fn_get_sorted_list_item ( TO_CHAR( a.OG_CALL_1,'yyyymmdd')||'|'||TO_CHAR( a.OG_CALL_2,'yyyymmdd')||'|'|| TO_CHAR( a.OG_CALL_3,'yyyymmdd')||'|'||TO_CHAR( a.OG_CALL_4,'yyyymmdd')||'|'||TO_CHAR( b.OG_CALL_1,'yyyymmdd')||'|'||TO_CHAR( b.OG_CALL_2,'yyyymmdd')||'|'|| TO_CHAR( b.OG_CALL_3,'yyyymmdd')||'|'||TO_CHAR( b.OG_CALL_4,'yyyymmdd'), 2 , 'DESC', '|', ':'), 'yyyymmdd')OG_CALL_3,
    IF(a.msisdn IS NULL OR b.msisdn IS NULL , nvl(a.OG_CALL_4, b.OG_CALL_4),TO_DATE ( mon.fn_get_sorted_list_item ( TO_CHAR( a.OG_CALL_1,'yyyymmdd')||'|'||TO_CHAR( a.OG_CALL_2,'yyyymmdd')||'|'|| TO_CHAR( a.OG_CALL_3,'yyyymmdd')||'|'||TO_CHAR( a.OG_CALL_4,'yyyymmdd')||'|'||TO_CHAR( b.OG_CALL_1,'yyyymmdd')||'|'||TO_CHAR( b.OG_CALL_2,'yyyymmdd')||'|'|| TO_CHAR( b.OG_CALL_3,'yyyymmdd')||'|'||TO_CHAR( b.OG_CALL_4,'yyyymmdd'), 1 , 'DESC', '|', ':'), 'yyyymmdd')  OG_CALL_4,
    IF(a.msisdn IS NULL OR b.msisdn IS NULL , nvl(a.IC_CALL_1, b.IC_CALL_1),TO_DATE ( mon.fn_get_sorted_list_item ( TO_CHAR( a.IC_CALL_1,'yyyymmdd')||'|'||TO_CHAR( a.IC_CALL_2,'yyyymmdd')||'|'|| TO_CHAR( a.IC_CALL_3,'yyyymmdd')||'|'||TO_CHAR( a.IC_CALL_4,'yyyymmdd')||'|'||TO_CHAR( b.IC_CALL_1,'yyyymmdd')||'|'||TO_CHAR( b.IC_CALL_2,'yyyymmdd')||'|'|| TO_CHAR( b.IC_CALL_3,'yyyymmdd')||'|'||TO_CHAR( b.IC_CALL_4,'yyyymmdd'), 4 , 'DESC', '|', ':'), 'yyyymmdd')     ) IC_CALL_1,
	IF(a.msisdn IS NULL OR b.msisdn IS NULL , nvl(a.IC_CALL_2, b.IC_CALL_2),TO_DATE ( mon.fn_get_sorted_list_item ( TO_CHAR( a.IC_CALL_1,'yyyymmdd')||'|'||TO_CHAR( a.IC_CALL_2,'yyyymmdd')||'|'|| TO_CHAR( a.IC_CALL_3,'yyyymmdd')||'|'||TO_CHAR( a.IC_CALL_4,'yyyymmdd')||'|'||TO_CHAR( b.IC_CALL_1,'yyyymmdd')||'|'||TO_CHAR( b.IC_CALL_2,'yyyymmdd')||'|'|| TO_CHAR( b.IC_CALL_3,'yyyymmdd')||'|'||TO_CHAR( b.IC_CALL_4,'yyyymmdd'), 3 , 'DESC', '|', ':'), 'yyyymmdd')) IC_CALL_2,
	IF(a.msisdn IS NULL OR b.msisdn IS NULL , nvl(a.IC_CALL_3, b.IC_CALL_3),TO_DATE ( mon.fn_get_sorted_list_item ( TO_CHAR( a.IC_CALL_1,'yyyymmdd')||'|'||TO_CHAR( a.IC_CALL_2,'yyyymmdd')||'|'|| TO_CHAR( a.IC_CALL_3,'yyyymmdd')||'|'||TO_CHAR( a.IC_CALL_4,'yyyymmdd')||'|'||TO_CHAR( b.IC_CALL_1,'yyyymmdd')||'|'||TO_CHAR( b.IC_CALL_2,'yyyymmdd')||'|'|| TO_CHAR( b.IC_CALL_3,'yyyymmdd')||'|'||TO_CHAR( b.IC_CALL_4,'yyyymmdd'), 2 , 'DESC', '|', ':'), 'yyyymmdd')) IC_CALL_3,
	IF(a.msisdn IS NULL OR b.msisdn IS NULL , nvl(a.IC_CALL_4, b.IC_CALL_4),TO_DATE ( mon.fn_get_sorted_list_item ( TO_CHAR( a.IC_CALL_1,'yyyymmdd')||'|'||TO_CHAR( a.IC_CALL_2,'yyyymmdd')||'|'|| TO_CHAR( a.IC_CALL_3,'yyyymmdd')||'|'||TO_CHAR( a.IC_CALL_4,'yyyymmdd')||'|'||TO_CHAR( b.IC_CALL_1,'yyyymmdd')||'|'||TO_CHAR( b.IC_CALL_2,'yyyymmdd')||'|'|| TO_CHAR( b.IC_CALL_3,'yyyymmdd')||'|'||TO_CHAR( b.IC_CALL_4,'yyyymmdd'), 1 , 'DESC', '|', ':'), 'yyyymmdd')) IC_CALL_4,
	IF (a.msisdn IS NULL OR b.msisdn IS NULL , a.STATUS_1_MONTH, (CASE WHEN GREATEST(NVL(b.OG_CALL_4,to_date('19700101','yyyymmdd')),NVL(b.IC_CALL_4,to_date('19700101','yyyymmdd')))>=ADD_MONTHS(TO_DATE (s_slice_value, 'yyyymmdd'),-1) THEN 'ACTIVE' ELSE 'INACTIVE' END)) STATUS_1_MONTH
	IF (a.msisdn IS NULL OR b.msisdn IS NULL , a.STATUS_2_MONTH, (CASE WHEN GREATEST(NVL(b.OG_CALL_4,to_date('19700101','yyyymmdd')),NVL(b.IC_CALL_4,to_date('19700101','yyyymmdd')))>=ADD_MONTHS(TO_DATE (s_slice_value, 'yyyymmdd'),-2) THEN 'ACTIVE' ELSE 'INACTIVE' END)  STATUS_2_MONTH
	IF (a.msisdn IS NULL OR b.msisdn IS NULL , a.STATUS_3_MONTH, (CASE WHEN GREATEST(NVL(b.OG_CALL_4,to_date('19700101','yyyymmdd')),NVL(b.IC_CALL_4,to_date('19700101','yyyymmdd')))>=ADD_MONTHS(TO_DATE (s_slice_value, 'yyyymmdd'),-3) THEN 'ACTIVE' ELSE 'INACTIVE' END) ) STATUS_3_MONTH
	IF (a.msisdn IS NULL OR b.msisdn IS NULL , a.STATUS_6_MONTH, (CASE WHEN GREATEST(NVL(b.OG_CALL_4,to_date('19700101','yyyymmdd')),NVL(b.IC_CALL_4,to_date('19700101','yyyymmdd')))>=ADD_MONTHS(TO_DATE (s_slice_value, 'yyyymmdd'),-6) THEN 'ACTIVE' ELSE 'INACTIVE' END)) STATUS_6_MONTH
	IF (a.msisdn IS NULL OR b.msisdn IS NULL , a.STATUS_12_MONTH, (CASE WHEN GREATEST(NVL(b.OG_CALL_4,to_date('19700101','yyyymmdd')),NVL(b.IC_CALL_4,to_date('19700101','yyyymmdd')))>=ADD_MONTHS(TO_DATE (s_slice_value, 'yyyymmdd'),-12) THEN 'ACTIVE' ELSE 'INACTIVE' END)) STATUS_12_MONTH
	a.FIRST_ENTRY_DATE,
	a.SUB_OPERATOR_CODE NULL,
	a.OPERATOR_CODE,
	CURRENT_TIMESTAMP INSERT_DATE,
    nvl(a.EVENT_DATE, b.EVENT_DATE) EVENT_DATE

	FROM
MON.SPARK_TT_OPERATOR_ACCT_ACTIVITY A
FULL OUTER JOIN
(
    SELECT EVENT_DATE
            , MSISDN
            , max(OG_CALL_1)OG_CALL_1
            , max(OG_CALL_2)OG_CALL_2
            , max(OG_CALL_3)OG_CALL_3
            , max(OG_CALL_4)OG_CALL_4
            , max(IC_CALL_1)IC_CALL_1
            , max(IC_CALL_2)IC_CALL_2
            , max(IC_CALL_3)IC_CALL_3
            , max(IC_CALL_4)IC_CALL_4
            , max(OPERATOR_CODE)OPERATOR_CODE
       FROM MON.SPARK_FT_HUA_OPERATOR_OG_IC_SNAPSHOT
       WHERE EVENT_DATE=TO_DATE (s_slice_value, 'yyyymmdd')
       GROUP BY EVENT_DATE,MSISDN
) B

ON (  A.MSISDN = B.MSISDN )

WHERE a.EVENT_DATE=TO_DATE (s_slice_value, 'yyyymmdd')




