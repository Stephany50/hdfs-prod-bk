SELECT IF(COUNT(C.INDEX) > 0, 'NOK','OK') RESULT
 FROM
   (
      SELECT MSC_TYPE, MIN_IND + i INDEX
      FROM (
      (SELECT MSC_TYPE, MAX(A.IND) MAX_IND,MIN(A.IND) MIN_IND
       FROM (
            SELECT DISTINCT CAST(SUBSTRING(SOURCE,11,9) AS INT) IND, SUBSTRING(SOURCE,5,11) MSC_TYPE
            FROM TMP_IT_CRA_MSC_HUAWEI
            WHERE CALLDATE = '###SLICE_VALUE###'
            ) A
       GROUP BY MSC_TYPE) t
       LATERAL VIEW POSEXPLODE(split(space(MAX_IND - MIN_IND),' ')) pe as i,s) 
   )C
 WHERE
   NOT EXISTS
   (
      SELECT 1  FROM TMP_IT_CRA_MSC_HUAWEI B
      WHERE CALLDATE = '###SLICE_VALUE###' AND CAST(SUBSTRING(B.SOURCE,11,9) AS INT) = C.INDEX
   );
