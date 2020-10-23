CREATE VIEW MON.VW_DT_SITE_ZONE_PMO 
(SITE_NAME, VILLE, REGION, LONGITUDE, LATITUDE, NUMERO_ZONE, NOM_ZONE, TECHNOLOGIES, REGION_PMO) AS 
(
  SELECT  site AS site_name, 
          MAX(ville) AS ville, 
          MAX(region) AS region,
          MAX("LONG") AS longitude, 
          MAX(lat) AS latitude,
          ZONE AS numero_zone, 
          zone_lib AS nom_zone,
          CONCAT(MAX(is_techno_2g),'  ',MAX(is_techno_3g),'  ',MAX(is_techno_4g)) AS technologies, 
          MAX(REGION_PMO) region_pmo
  FROM (SELECT a.*, 
              (CASE
                 WHEN techno = '2G'
                    THEN '2G'
                 ELSE ''
              END) AS is_techno_2g,
              (CASE
                 WHEN techno = '3G'
                    THEN '3G'
                 ELSE ''
              END) AS is_techno_3g,
              (CASE
                 WHEN techno = '4G'
                    THEN '4G'
                 ELSE ''
              END) AS is_techno_4g
         FROM mondv.dt_site_zone a)
  GROUP BY site, ZONE, zone_lib
);