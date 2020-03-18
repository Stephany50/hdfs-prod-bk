--Fakes
--Fakes
  INSERT INTO MON.SPARK_FT_BUSINESS_ACTIVITY_DD
  (
    SELECT
        'DD_PMO_FAKE',b.zone_lib,sum(fake_act) fake,
        'FT_PARC_ACTIVATIONS_SITE', CURRENT_TIMESTAMP, a.event_date
    FROM (
       SELECT * FROM MON.SPARK_FT_PARC_ACTIVATIONS_SITE WHERE TO_DATE(EVENT_DATE)='###SLICE_VALUE###'
    ) a
    LEFT JOIN (select distinct zone_lib, site from DIM.DT_SITE_ZONE) b
    ON a.first_site_name = b.site
    GROUP BY a.event_date,b.zone_lib
   );