--Ratio Fakes/Activations
INSERT INTO MON.SPARK_FT_BUSINESS_ACTIVITY_DD
(
    SELECT
        'DD_PMO_RATIO_FAKE',b.zone_lib,sum(fake_act)*100 / sum(subscriber_count) ratio,
        'FT_PARC_ACTIVATIONS_SITE', CURRENT_TIMESTAMP, a.event_date
    FROM (
        SELECT *
        FROM MON.SPARK_FT_PARC_ACTIVATIONS_SITE
        WHERE to_date(EVENT_DATE)='###SLICE_VALUE###'
    ) a
    LEFT JOIN (
        SELECT DISTINCT zone_lib, site
        FROM DIM.DT_SITE_ZONE
    ) b
    ON a.first_site_name = b.site
    GROUP BY a.event_date,b.zone_lib
);