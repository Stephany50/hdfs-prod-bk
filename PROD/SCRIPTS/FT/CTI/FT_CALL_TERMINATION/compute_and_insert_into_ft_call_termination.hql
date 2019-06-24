INSERT INTO CTI.FT_CALL_TERMINATION
SELECT
 DATE_FORMAT(DATE_DEBUT_SVI,'yyyy') EVENT_YEAR,
 DATE_FORMAT(DATE_DEBUT_SVI,'yyyymm') EVENT_MONTH,
 DATE_FORMAT(DATE_DEBUT_SVI,'iw')EVENT_WEEK,
 timestamp(DATE_DEBUT_SVI) EVENT_DATE,
 S.ID_APPEL,
 msisdn,
 ELEMENT,
 date_element,
 date_hangup,
 type_hangup,
 current_timestamp insert_date,
 S.SEGMENT_CLIENT,
 S.DATE_DEBUT_SVI
FROM
(SELECT * FROM cti.svi_navigation  WHERE ORIGINAL_FILE_DATE = (select max(original_file_date) FROM cti.svi_navigation))N
INNER JOIN (SELECT * FROM cti.svi_appel WHERE ORIGINAL_FILE_DATE=(select max(original_file_date) FROM cti.svi_appel))S
ON (N.DATE_ELEMENT='###SLICE_VALUE###')
AND S.DATE_DEBUT_OMS='###SLICE_VALUE###'
AND N.ID_APPEL= S.ID_APPEL
AND type_appel ='SVI'
group by
 timestamp(DATE_DEBUT_SVI),
 S.ID_APPEL,
 msisdn,
 element,
 date_element,
 date_hangup,
 type_hangup,
 DATE_FORMAT (DATE_DEBUT_SVI,'iw'),
 DATE_FORMAT (DATE_DEBUT_SVI,'yyyymm'),
 DATE_FORMAT (DATE_DEBUT_SVI,'yyyy'),
 S.SEGMENT_CLIENT,
 S.DATE_DEBUT_SVI
having DATE_ELEMENT =max(DATE_ELEMENT) ;
