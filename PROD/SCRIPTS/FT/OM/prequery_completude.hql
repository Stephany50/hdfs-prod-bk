SELECT IF(COUNT(C.HOUR) > 0, 'NOK','OK') RESULT
 FROM
   (
      SELECT pe.i HOUR
      FROM (select 0 ) t 
      LATERAL VIEW POSEXPLODE(split(space(23),' ')) pe as i,s 
   )C
 WHERE
 NOT EXISTS
   (
      SELECT 1  
      FROM (
          SELECT DISTINCT CAST(DATE_FORMAT(${hivevar:event_datetime}, 'HH') AS INT) H
          FROM ${hivevar:table_name}
          WHERE ${hivevar:partition_column} = '###SLICE_VALUE###'
      ) A
      WHERE A.H = C.HOUR
   )

