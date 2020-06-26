create view VW_SITE_INFO as
(
SELECT site_name, townname, administrative_region, commercial_region
FROM (SELECT site_name, townname, region administrative_region,
commercial_region,
ROW_NUMBER () OVER (PARTITION BY site_name ORDER BY townname)
AS rang
FROM dim.dt_gsm_cell_code) T
WHERE rang = 1)