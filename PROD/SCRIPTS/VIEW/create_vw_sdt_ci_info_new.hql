CREATE VIEW IF NOT EXISTS VW_SDT_CI_INFO_NEW
(CI, TOWNNAME, SECTEUR, ADMINISTRATIVE_REGION, ZONE,
COMMERCIAL_REGION, CELL_NAME, SITE_NAME, SITE_CODE, TECHNOLOGIE)
AS 
  (SELECT a.ci, 
          a.townname, 
          a.secteur, 
        ( case
           when Upper (a.administrative_region)= 'NRO' then 'NORD-OUEST'
           when Upper (a.administrative_region)= 'EXN' then 'EXTREME-NORD'
           when Upper (a.administrative_region)= 'YDE' then 'CENTRE'
           when Upper (a.administrative_region)= 'SUO' then 'SUD-OUEST'
           when Upper (a.administrative_region)= 'ADM' then 'ADAMAOUA'
           when Upper (a.administrative_region)= 'CTR' then 'CENTRE'
           when Upper (a.administrative_region)= 'NRD' then 'NORD'
           when Upper (a.administrative_region)= 'DLA' then 'LITTORAL'
           when Upper (a.administrative_region)= 'OST' then 'OUEST'
           else Upper (a.administrative_region)
           end)
          administrative_region, 
          b.zone, 
          a.commercial_region, 
          a.cell_name, 
          a.site_name, 
          a.site_code, 
          a.technologie 
   FROM   (SELECT a.ci, 
                  Upper (a.townname)          townname, 
                  Upper (b.secteur)           secteur, 
                  Upper (g.region)            administrative_region, 
                  Upper (c.zone)              ZONE, 
                  Upper (d.commercial_region) commercial_region, 
                  Upper (e.cellname)          cell_name, 
                  Upper (f.site_name)         site_name, 
                  Upper (h.site_code)         site_code, 
                  Upper (i.technologie)       technologie 
           FROM   (SELECT DISTINCT b.ci ci, First_value (b.townname)  over (PARTITION BY b.ci ORDER BY b.nbre DESC) townname
                   FROM   (SELECT b.ci, Replace (Replace (b.townname, '¿', 'e'), 'é' , 'e') townname, SUM (1) nbre FROM   dim.dt_gsm_cell_code b
                   GROUP  BY b.ci,Replace (Replace (b.townname, '¿', 'e'),'é', 'e')) b) a
                 LEFT JOIN  (SELECT DISTINCT b.ci ci, First_value (b.secteur) over ( PARTITION BY b.ci ORDER BY b.nbre DESC) secteur
                   FROM   (SELECT b.ci, b.secteur, SUM (1) nbre
                           FROM   dim.dt_gsm_cell_code b  GROUP  BY b.ci, b.secteur) b) b
                       ON (a.ci = b.ci)
             LEFT JOIN     (SELECT DISTINCT b.ci  ci, First_value (b.zone)  over ( PARTITION BY b.ci ORDER BY b.nbre DESC) ZONE
                   FROM   (SELECT b.ci,  b.zone,  SUM (1) nbre
                           FROM   dim.dt_gsm_cell_code b  GROUP  BY b.ci,  b.zone) b) c
                       ON (a.ci = c.ci)
             LEFT JOIN     (SELECT DISTINCT b.ci  ci, First_value (b.commercial_region) over (PARTITION BY b.ci ORDER BY b.nbre DESC) commercial_region
                   FROM   (SELECT b.ci,  b.commercial_region, SUM (1) nbre
                   FROM   dim.dt_gsm_cell_code b GROUP  BY b.ci, b.commercial_region) b) d
                       ON(a.ci = d.ci)
             LEFT JOIN     (SELECT DISTINCT b.ci ci,  First_value (b.cellname) over ( PARTITION BY b.ci  ORDER BY b.nbre DESC) cellname
                   FROM   (SELECT b.ci, b.cellname,   SUM (1) nbre   FROM   dim.dt_gsm_cell_code b GROUP  BY b.ci, b.cellname) b) e
                       ON(a.ci = e.ci)
             LEFT JOIN     (SELECT DISTINCT b.ci  ci, First_value (b.site_name) over ( PARTITION BY b.ci ORDER BY b.nbre DESC) site_name
                   FROM   (SELECT b.ci, b.site_name, SUM (1) nbre   FROM  dim.dt_gsm_cell_code b GROUP  BY b.ci,  b.site_name) b) f
                        ON(a.ci = f.ci)
             LEFT JOIN     (SELECT DISTINCT b.ci ci, First_value (b.region) over ( PARTITION BY b.ci  ORDER BY b.nbre DESC) region
                   FROM   (SELECT b.ci, b.region, SUM (1) nbre   FROM   dim.dt_gsm_cell_code b GROUP  BY b.ci, b.region) b) g
                        ON(a.ci = g.ci)
             LEFT JOIN     (SELECT DISTINCT b.ci  ci, First_value (b.site_code)  over (  PARTITION BY b.ci  ORDER BY b.nbre DESC) site_code
                   FROM   (SELECT b.ci, b.site_code, SUM (1) nbre FROM dim.dt_gsm_cell_code b   GROUP  BY b.ci, b.site_code) b) h
                        ON(a.ci = h.ci)
             LEFT JOIN     (SELECT DISTINCT b.ci ci, First_value (b.technologie) over ( PARTITION BY b.ci   ORDER BY b.nbre DESC) technologie
                   FROM   (SELECT b.ci, b.technologie, SUM (1) nbre  FROM dim.dt_gsm_cell_code b GROUP  BY b.ci, b.technologie) b) i
                        ON(a.ci = i.ci)) a
         LEFT JOIN (SELECT Upper (Trim (cellname)) cellname, Max (zone) ZONE
           FROM   dim.dt_gsm_zone_code 
           GROUP  BY Upper (Trim (cellname))) b
            ON(a.cell_name = b.cellname));