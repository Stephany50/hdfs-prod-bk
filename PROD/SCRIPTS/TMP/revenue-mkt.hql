CREATE EXTERNAL TABLE CDR.SPARK_TT_BUDGET_GLOBAL
(
  EVENT_DATE     DATE,
  valeur  DOUBLE,
  TYPE  VARCHAR(50)
)
COMMENT 'external tables-TT'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION '/PROD/TT/BUDGET/GLOBAL'
TBLPROPERTIES ('serialization.null.format'='')
;

CREATE EXTERNAL TABLE CDR.SPARK_TT_BUDGET_REGIONAL_GA
(
  EVENT_DATE     DATE,
  valeur  DOUBLE,
  TYPE  VARCHAR(50)
)
COMMENT 'external tables-TT'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION '/PROD/TT/BUDGET/REGIONAL_GA'
TBLPROPERTIES ('serialization.null.format'='')
;

CREATE EXTERNAL TABLE CDR.SPARK_TT_BUDGET_REGIONAL_DE
(
  EVENT_DATE     DATE,
  valeur  DOUBLE,
  TYPE  VARCHAR(50)
)
COMMENT 'external tables-TT'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION '/PROD/TT/BUDGET/REGIONAL_DE'
TBLPROPERTIES ('serialization.null.format'='')
;
kpi	valeur	region	global	vs

CREATE EXTERNAL TABLE CDR.SPARK_TT_BUDGET_REGIONAL_REVENU2
(
  mois     VARCHAR(7),
  kpi     VARCHAR(50),
  valeur  DOUBLE,
  region  VARCHAR(50),
  global  DOUBLE,
  vs  DOUBLE
)
COMMENT 'external tables-TT'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\00204F'
LOCATION '/PROD/TT/BUDGET/REGIONAL_REVENU'
TBLPROPERTIES ('serialization.null.format'='')
;







insert into   REVENNE_DATA
SELECT
 mois,
TRIM(loc_admintrative_region) loc_admintrative_region,
AVG(Souscription_Data +principal_Data) REVENU_DATA ,
AVG(Souscription_Data +  principal_Data)/31 REVENU_DATA_jour
FROM(
        select

       TRIM(loc_admintrative_region) loc_admintrative_region,
        sum (c.data) as Souscription_Data,
        mois,
        sum (d.DATA_MAIN_RATED_AMOUNT) as principal_Data

        from
            (select TO_CHAR(period,'yyyymm') as mois,
            msisdn,
            sum (nvl(bdle_cost*COEFF_DATA/100,0) +nvl(bdle_cost*COEFF_ROAMING_DATA/100,0)) as Data
            from  mon.FT_CBM_BUNDLE_SUBS_DAILY a, dim.ref_souscription b
                  where period between '01/11/2018' and LAST_DAY('01/12/2018')
                  and upper(trim(a.bdle_name))= upper(trim(b.bdle_name))
                  group by TO_CHAR(period,'yyyymm'), msisdn,TO_CHAR(period,'yyyymm') ) c,
            (select distinct event_month, DATA_MAIN_RATED_AMOUNT,TRIM(loc_admintrative_region) loc_admintrative_region,msisdn
            from MON.FT_MARKETING_DATAMART_MONTH
            where event_month between '201811' and '201812' ) d
    where
    c.msisdn=d.msisdn and
    c.mois=d.event_month
    group by TRIM(loc_admintrative_region),mois

    )
GROUP BY mois,
TRIM(loc_admintrative_region);



SELECT
     'PARC_OM_30Jrs' KPI
     , COUNT (DISTINCT b2.MSISDN) parc
     , townname
     , b2.month
FROM (select distinct msisdn , substring(JOUR,1,7) month  from MON.SPARK_DATAMART_OM_MARKETING2 WHERE JOUR BETWEEN '2020-04-01' and '2020-05-31' AND STYLE NOT LIKE ('REC%') )B2
left join (
    select
        a.msisdn,
        substring(a.event_date,1,7) month,
        max(nvl(a.townname,b.townname)) townname
    from mon.spark_ft_client_last_site_day a
    left join (
        select * from mon.spark_ft_client_site_traffic_day where event_date in ( '2020-04-30', '2020-05-31')
    ) b on a.msisdn = b.msisdn and substring(a.event_date,1,7)=substring(b.event_date,1,7)
    where a.event_date in ( '2020-04-30', '2020-05-31')
    group by a.msisdn,substring(a.event_date,1,7)
) site on  site.msisdn =B2.msisdn and site.month =b2.month
GROUP BY
    townname,
    b2.month



SELECT
     'REVENUE_OM' KPI
     , SUM(nvl(REVENU,0)) rev
     , townname
     , substring(b2.jour ,1,7) month
FROM MON.SPARK_DATAMART_OM_MARKETING2 B2
left join (
    select
        a.msisdn,
        substring(a.event_date,1,7) month,
        max(nvl(a.townname,b.townname)) townname
    from mon.spark_ft_client_last_site_day a
    left join (
        select * from mon.spark_ft_client_site_traffic_day where event_date in ( '2020-04-30', '2020-05-31')
    ) b on a.msisdn = b.msisdn
    where a.event_date in ( '2020-04-30', '2020-05-31')
    group by a.msisdn,substring(a.event_date,1,7)
) site on  site.msisdn =B2.msisdn and site.month =substring(b2.jour ,1,7)
WHERE JOUR BETWEEN '2020-04-01' and '2020-05-31'--and STYLE  IN ('RECHARGE','TOP_UP')
GROUP BY
      townname,
    substring(b2.jour ,1,7)
