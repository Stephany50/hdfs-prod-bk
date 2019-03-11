------****************************************************************************
------*********************MSC PRE-QUERY **************************************   
 
SELECT COUNT(C.INDEX) MISSING_INDEXES
 FROM
 ( SELECT MSC_TYPE, MIN_IND + i INDEX
	FROM (
 (SELECT MSC_TYPE, MAX(A.IND) MAX_IND,MIN(A.IND) MIN_IND
	 FROM (
	 SELECT DISTINCT CAST(SUBSTRING(SOURCE,11,9) AS INT) IND, SUBSTRING(SOURCE,1,11) MSC_TYPE
	 FROM CDR.IT_CRA_MSC_HUAWEI
	 WHERE CALLDATE = '${hivevar:date}'
	 ) A
	 GROUP BY MSC_TYPE) t
	 LATERAL VIEW POSEXPLODE(split(space(MAX_IND - MIN_IND),' ')) pe as i,s) 
	 )C
 WHERE
 NOT EXISTS
 (
 SELECT 1 FROM CDR.IT_CRA_MSC_HUAWEI B
 WHERE CALLDATE = '${hivevar:date}' AND CAST(SUBSTRING(B.SOURCE,11,9) AS INT) = C.INDEX
 )
;
