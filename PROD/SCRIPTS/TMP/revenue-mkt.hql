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