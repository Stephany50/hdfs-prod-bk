INSERT INTO cti.ft_call_termination
SELECT Date_format(date_debut_svi, 'yyyy')   EVENT_YEAR,
       Date_format(date_debut_svi, 'yyyymm') EVENT_MONTH,
       Date_format(date_debut_svi, 'iw')     EVENT_WEEK,
       Timestamp(date_debut_svi)             EVENT_DATE,
       S.id_appel,
       msisdn,
       element,
       date_element,
       date_hangup,
       type_hangup,
       CURRENT_TIMESTAMP                     insert_date,
       S.segment_client,
       S.date_debut_svi
FROM   cti.svi_navigation N
       INNER JOIN cti.svi_appel S
               ON ( N.id_appel = S.id_appel )
WHERE  N.date_element = '###SLICE_VALUE###'
       AND S.date_debut_oms = '###SLICE_VALUE###'
       AND type_appel = 'SVI'
GROUP  BY Timestamp(date_debut_svi),
          S.id_appel,
          msisdn,
          element,
          date_element,
          date_hangup,
          type_hangup,
          Date_format (date_debut_svi, 'iw'),
          Date_format (date_debut_svi, 'yyyymm'),
          Date_format (date_debut_svi, 'yyyy'),
          S.segment_client,
          S.date_debut_svi
HAVING date_element = Max(date_element);