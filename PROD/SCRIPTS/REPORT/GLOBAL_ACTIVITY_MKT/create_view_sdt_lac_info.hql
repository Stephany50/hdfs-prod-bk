
CREATE  VIEW MON.VW_SDT_LAC_INFO  AS
SELECT
   a.lac LAC,
   UPPER (a.townname) townname,
   UPPER (b.secteur) secteur,
   UPPER (c.ZONE) ZONE,
   CASE
        WHEN UPPER (e.region)  = 'NRO' THEN 'NORD-OUEST'
        WHEN UPPER (e.region)  =  'EXN' THEN 'EXTREME-NORD'
        WHEN UPPER (e.region)  =  'YDE' THEN 'CENTRE'
        WHEN UPPER (e.region)  =  'SUO' THEN 'SUD-OUEST'
        WHEN UPPER (e.region)  =  'ADM' THEN 'ADAMAOUA'
        WHEN UPPER (e.region)  =  'CTR'THEN 'CENTRE'
        WHEN UPPER (e.region)  =  'NRD' THEN 'NORD'
        WHEN UPPER (e.region)  =  'DLA' THEN 'LITTORAL'
        WHEN UPPER (e.region)  =  'OST' THEN 'OUEST'
        ELSE  UPPER (e.region)
    END administrative_region,
   UPPER (d.commercial_region) commercial_region
FROM (
    SELECT
        DISTINCT b.lac lac,
        FIRST_VALUE (b.townname) OVER (PARTITION BY b.lac ORDER BY b.nbre DESC) townname
   FROM (
        SELECT
            b.lac,
            REPLACE (REPLACE (b.townname, '¿', 'e'), 'é', 'e' ) townname,
            SUM (1) nbre
        FROM dim.dt_gsm_cell_code b
        GROUP BY
            b.lac,
            REPLACE (REPLACE (b.townname, '¿', 'e'), 'é', 'e' )
    ) b
) a
LEFT JOIN (
    SELECT
        DISTINCT b.lac lac,
        FIRST_VALUE (b.secteur) OVER (PARTITION BY b.lac ORDER BY b.nbre DESC) secteur
    FROM (
        SELECT
            b.lac,
            b.secteur,
            SUM (1) nbre
         FROM dim.dt_gsm_cell_code b
        GROUP BY b.lac, b.secteur
    ) b
) b on  a.lac = b.lac
LEFT JOIN (
    SELECT
        DISTINCT b.lac lac,
        FIRST_VALUE (b.ZONE) OVER (PARTITION BY b.lac ORDER BY b.nbre DESC) ZONE
    FROM (
        SELECT
            b.lac,
            b.ZONE,
            SUM (1) nbre
        FROM dim.dt_gsm_cell_code b
        GROUP BY b.lac, b.ZONE
    ) b
) c on  a.lac = c.lac
LEFT JOIN(
    SELECT
        DISTINCT b.lac lac,
        FIRST_VALUE (b.commercial_region) OVER (PARTITION BY b.lac ORDER BY b.nbre DESC) commercial_region
    FROM (
        SELECT
            b.lac,
            b.commercial_region,
            SUM (1) nbre
         FROM dim.dt_gsm_cell_code b
     GROUP BY b.lac, b.commercial_region
    ) b
) d on  a.lac = d.lac
LEFT JOIN(
    SELECT
        DISTINCT b.lac lac,
        FIRST_VALUE (b.region) OVER (PARTITION BY b.lac ORDER BY b.nbre DESC) region
    FROM (
        SELECT
            b.lac,
            b.region,
            SUM (1) nbre
        FROM dim.dt_gsm_cell_code b
        GROUP BY b.lac, b.region
    ) b
) e on  a.lac = e.lac
