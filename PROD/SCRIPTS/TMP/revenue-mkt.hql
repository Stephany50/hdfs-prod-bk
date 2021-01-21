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

CREATE EXTERNAL TABLE CDR.SPARK_TT_BUDGET_GLOBAL2
(
  EVENT_DATE     DATE,
  TYPE  VARCHAR(50),
    valeur  DOUBLE
)
COMMENT 'external tables-TT'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\;'
LOCATION '/PROD/TT/BUDGET/GLOBAL2'
TBLPROPERTIES ('serialization.null.format'='')
;
CREATE EXTERNAL TABLE CDR.SPARK_TT_BUDGET_GLOBAL_POIDS
(
  mois     varchar(7),
  region  VARCHAR(50),
  kpi  VARCHAR(50),
   poids  DOUBLE
)
COMMENT 'external tables-TT'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\;'
LOCATION '/PROD/TT/BUDGET/GLOBAL_POIDS'
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


CREATE EXTERNAL TABLE CDR.SPARK_TT_BUDGET_REGIONAL_DE2
(
  EVENT_DATE     DATE,
  valeur  DOUBLE,
  region  VARCHAR(50)
)
COMMENT 'external tables-TT'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\;'
LOCATION '/PROD/TT/BUDGET/REGIONAL_DE2'
TBLPROPERTIES ('serialization.null.format'='')

CREATE EXTERNAL TABLE CDR.SPARK_TT_BUDGET_REVENU_OM_2021
(
  EVENT_DATE     DATE,
  valeur  DOUBLE,
  region  VARCHAR(50)
)
COMMENT 'external tables-TT'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\;'
LOCATION '/PROD/TT/BUDGET/REVENU_OM_2021'
TBLPROPERTIES ('serialization.null.format'='')


CREATE EXTERNAL TABLE CDR.SPARK_TT_BUDGET_TELCO_2021
(
  EVENT_DATE     DATE,
  valeur  DOUBLE,
  KPI VARCHAR(50),
  region  VARCHAR(50)
)
COMMENT 'external tables-TT'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\;'
LOCATION '/PROD/TT/BUDGET/REVENU_TELCO_2021'
TBLPROPERTIES ('serialization.null.format'='')

CREATE EXTERNAL TABLE CDR.SPARK_TT_BUDGET_SUBSCRIBERS_2021
(
  EVENT_DATE     DATE,
  valeur  DOUBLE,
  region  VARCHAR(50),
   KPI VARCHAR(50)
)
COMMENT 'external tables-TT'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\;'
LOCATION '/PROD/TT/BUDGET/REVENU_SUBSCRIBERS_2021'
TBLPROPERTIES ('serialization.null.format'='')


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


CREATE EXTERNAL TABLE CDR.TT_REF_SUBS (
bdle_name STRING ,
prix STRING ,
coeff_onnet STRING ,
coeff_offnet STRING ,
coeff_inter STRING ,
coeff_data STRING ,
coeff_roaming_voix STRING ,
coeff_roaming_data STRING ,
validite STRING ,
Type_forfait STRING ,
destination STRING ,
type_ocm STRING ,
Offre STRING ,
offer STRING ,
offer_1 STRING ,
offer_2 STRING ,
COMBO STRING)
COMMENT 'external tables-TT'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\;'
LOCATION '/DATALAB/TT/REF_SUBS'
TBLPROPERTIES ('serialization.null.format'='')
;
if (a.event is not null and b.event is not null , b.$1,nvl(a.$1,b.$1) ) $1

create external  table CDR.TT_REF_SUBS2 (
bdle_name STRING,
prix STRING,
coeff_onnet STRING,
coeff_offnet STRING,
coeff_inter STRING,
coeff_data STRING,
Coef_sms STRING,
Coef_sva STRING,
coeff_roaming STRING,
coeff_roaming_voix STRING,
coeff_roaming_data STRING,
coef_roaming_sms STRING,
validite STRING,
Type_forfait STRING,
destination STRING,
type_ocm STRING,
Offre STRING,
offer STRING,
offer_1 STRING,
offer_2 STRING,
COMBO STRING
)
COMMENT 'external tables-TT'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\;'
LOCATION '/DATALAB/TT/REF_SUBS2'
TBLPROPERTIES ('serialization.null.format'='')
;
nvl(voix_onnet,0)+nvl(voix_offnet,0)+nvl(voix_inter,0)+nvl(voix_roaming,0)+nvl(sms_onnet,0)+nvl(sms_offnet,0)+nvl(sms_inter,0)+nvl(sms_roaming,0)+nvl(data_bundle,0)+nvl(sva)
create table tmp.dt_services as
select 
    trim(nvl(b.event,a.event)) event,
    nvl(b.event_source,a.event_source)  event_source,
    nvl(b.service_code,a.service_code)  service_code,
    nvl(b.description,a.description)  description,
    nvl(b.insert_date,a.insert_date)  insert_date,
    nvl(b.family_mkt,a.family_mkt)  family_mkt,
    nvl(b.family_eoy,a.family_eoy)  family_eoy,
    nvl(b.validity,a.validity)  validity,
    nvl(b.usage,a.usage)  usage,
    nvl(b.flybox_dongle,a.flybox_dongle)  flybox_dongle,
    nvl(b.offres_b_to_b,a.offres_b_to_b)  offres_b_to_b,
    nvl(b.usage_mkt,a.usage_mkt)  usage_mkt,
    nvl(b.produit,a.produit)  produit,
    nvl(b.voix_onnet,a.voix_onnet)  voix_onnet,
    nvl(b.voix_offnet,a.voix_offnet)  voix_offnet,
    nvl(b.voix_inter,a.voix_inter)  voix_inter,
    nvl(b.voix_roaming,a.voix_roaming)  voix_roaming,
    nvl(b.sms_onnet,a.sms_onnet)  sms_onnet,
    nvl(b.sms_offnet,a.sms_offnet)  sms_offnet,
    nvl(b.sms_inter,a.sms_inter)  sms_inter,
    nvl(b.sms_roaming,a.sms_roaming)  sms_roaming,
    nvl(b.data_bundle,a.data_bundle)  data_bundle,
    nvl(b.sva,a.sva)  sva,
    nvl(b.validite,a.validite)  validite,
    nvl(b.marche,a.marche)  marche,
    nvl(b.segment,a.segment)  segment,
    nvl(b.family_mktsubs,a.family_mktsubs)  family_mktsubs,
    nvl(b.family_magicbundles,a.family_magicbundles)  family_magicbundles,
    nvl(b.prix,a.prix)  prix,
    nvl(b.type_ocm,a.type_ocm)  type_ocm,
    nvl(b.combo,a.combo)  combo
from (select * from (select a.*, row_number() over (partition by upper(trim(event)) order by insert_date desc )rg from DIM.dt_services a) a where rg=1) a
full outer join (
    SELECT
        event,
        NULL event_source,
        NULL service_code,
        NULL description,
        CURRENT_TIMESTAMP insert_date,
        NULL family_mkt,
        NULL family_eoy,
        NULL validity,
        NULL usage,
        NULL flybox_dongle,
        NULL offres_b_to_b,
        NULL usage_mkt,
        NULL produit,
        voix_onnet,
        voix_offnet,
        voix_inter,
        voix_roaming,
        sms_onnet,
        sms_offnet,
        sms_inter,
        sms_roaming,
        data_bundle,
        sva sva,
        validite,
        NULL marche,
        NULL segment,
        NULL family_mktsubs,
        NULL family_magicbundles,
        prix,
        type_ocm,
        combo
    from (
         select
            bdle_name event,
            prix,
            (case when trim(upper(type_forfait))<>upper('SMS') then coeff_onnet/100 else 0 end) voix_onnet,
            (case when trim(upper(type_forfait))<>upper('SMS') then coeff_offnet/100 else 0 end) voix_offnet,
            (case when trim(upper(type_forfait))<>upper('SMS') then coeff_inter/100 else 0 end)  voix_inter,
            (case when trim(upper(type_forfait))<>upper('SMS') then coeff_roaming_voix/100 else 0 end)  voix_roaming,
            (case when trim(upper(type_forfait))=upper('SMS') then coeff_onnet/100 else 0 end) sms_onnet,
            (case when trim(upper(type_forfait))=('SMS') then coeff_offnet/100 else 0 end) sms_offnet,
            (case when trim(upper(type_forfait))=upper('SMS') then coeff_inter/100 else 0 end)  sms_inter,
            (case when trim(upper(type_forfait))=upper('SMS') then coeff_roaming_voix/100 else 0 end)  sms_roaming,
            (case when upper(trim(produit)<>'SVA' then (nvl(coeff_data,0,0)+nvl(coeff_roaming_data,0))/100 else 0 end ) data_bundle ,
            (case when upper(trim(produit)='SVA' then (nvl(coeff_data,0,0)+nvl(coeff_roaming_data,0))/100 else 0 end ) sva ,
            combo ,
            type_ocm ,
            validite ,
            row_number() over(partition by trim(upper(bdle_name)) order by coeff_inter desc )  rg
        from CDR.TT_REF_SUBS
    )REF where rg=1 and event is not null
) b on lower(trim(a.event))=lower(trim(b.event))

insert into junk.PLIT_JOUR_KPIS_OM
select
A.jour jour,
A.region_administrative region_administrative,
A.region_commerciale region_commerciale,
B.budget_cash_in budget_cash_in,
B.budget_cash_out budget_cash_out,
B.budget_merch_bill_pay budget_merch_bill_pay,
B.budget_revenu budget_revenu,
B.budget_cash_in*A.poids_cash_in_jour_mois  budget_jour_cash_in,
B.budget_cash_out*A.poids_cash_out_jour_mois  budget_jour_cash_out,
B.budget_merch_bill_pay*(A.poids_merch_pay_jour_mois+A.poids_bill_pay_jour_mois)/2   budget_jour_merch_bill_pay,
B.budget_revenu*A.poids_revenu_jour_mois  budget_jour_revenu
FROM
(select
*
from junk.poids_kpi_om4) A
left join
(select
case when mois='Juillet' then 07 WHEN mois='Septembre' THEN 09 WHEN mois='Octobre' then 10 WHEN mois='Novembre' THEN 11 WHEN mois='Août' THEN 08 when mois='Décembre' then 12 ELSE NULL END jour,
produit,
objectif,
CASE WHEN produit='VALEUR CI' THEN objectif ELSE 0 END budget_cash_in,
CASE WHEN produit='CASH OUT' THEN objectif ELSE 0 END budget_cash_out,
CASE WHEN produit='PAYEMENT' THEN objectif ELSE 0 END budget_merch_bill_pay,
CASE WHEN produit='REVENU OM' THEN objectif ELSE 0 END budget_revenu
from TMP.BUDGET_OM) B
ON(substr(A.jour,6,2)=B.jour)
WHERE B.jour IS NOT NULL


insert into junk.PLIT_JOUR_KPIS_OM
select
A.jour jour,
A.region_administrative region_administrative,
A.region_commerciale region_commerciale,
B.budget_cash_in budget_cash_in,
B.budget_cash_out budget_cash_out,
B.budget_merch_bill_pay budget_merch_bill_pay,
B.budget_revenu budget_revenu,
B.budget_cash_in*A.poids_cash_in_jour_mois  budget_jour_cash_in,
B.budget_cash_out*A.poids_cash_out_jour_mois  budget_jour_cash_out,
B.budget_merch_bill_pay*(A.poids_merch_pay_jour_mois+A.poids_bill_pay_jour_mois)/2   budget_jour_merch_bill_pay,
B.budget_revenu*A.poids_revenu_jour_mois  budget_jour_revenu
FROM
(select
*
from junk.poids_kpi_om4) A
left join
(select
case when mois='Juillet' then 07 WHEN mois='Septembre' THEN 09 WHEN mois='Octobre' then 10 WHEN mois='Novembre' THEN 11 WHEN mois='Août' THEN 08 when mois='Decembre' then 12 ELSE NULL END jour,
produit,
objectif,
CASE WHEN produit='VALEUR CI' THEN objectif ELSE 0 END budget_cash_in,
CASE WHEN produit='CASH OUT' THEN objectif ELSE 0 END budget_cash_out,
CASE WHEN produit='PAYEMENT' THEN objectif ELSE 0 END budget_merch_bill_pay,
CASE WHEN produit='REVENU OM' THEN objectif ELSE 0 END budget_revenu
from TMP.BUDGET_OM2 ) B
ON(substr(A.jour,6,2)=B.jour)
WHERE B.jour IS NOT NULL


------------------------------ consolidation avec les jours de la semaine -------------------------------
create table junk.poids_kpi_om_final as
select
jour ,
    jour_semaine,
    mois,
    region_administrative,
    region_commerciale,
    REVENU,
    CASH_IN,
    CASH_OUT,
    MERCH_PAY,
    BILL_PAY,
    nb_occ_jour_semaine,
    REVENU_MOIS,
    revenu_jour_semaine,
    poids_revenu/ (sum(poids_revenu) over (partition by mois )) poids_revenu,
    CASH_IN_MOIS,
    CASH_IN_jour_semaine,
    poids_CASH_IN / (sum(poids_CASH_IN) over (partition by mois )) poids_CASH_IN,
    CASH_OUT_MOIS,
    CASH_OUT_jour_semaine,
    poids_CASH_OUT/ (sum(poids_CASH_OUT) over (partition by mois )) poids_CASH_OUT,
    MERCH_PAY_MOIS,
    MERCH_PAY_jour_semaine,
    poids_MERCH_PAY /(sum(poids_MERCH_PAY) over (partition by mois )) poids_MERCH_PAY ,
    BILL_PAY_MOIS,
    BILL_PAY_jour_semaine,
    poids_BILL_PAY/(sum(poids_BILL_PAY) over (partition by mois ))  poids_BILL_PAY

from (
    select
      jour ,
        jour_semaine,
        mois,
        region_administrative,
        region_commerciale,
        REVENU,
        CASH_IN,
        CASH_OUT,
        MERCH_PAY,
        BILL_PAY,
        nb_occ_jour_semaine,
        REVENU_MOIS,
        revenu_jour_semaine,
        poids_revenu_jour_mois*poids_revenu_jour_semaine poids_revenu,
        CASH_IN_MOIS,
        CASH_IN_jour_semaine,
        poids_CASH_IN_jour_mois*poids_CASH_IN_jour_semaine poids_CASH_IN,
        CASH_OUT_MOIS,
        CASH_OUT_jour_semaine,
        poids_CASH_OUT_jour_mois*poids_CASH_OUT_jour_semaine poids_CASH_OUT,
        MERCH_PAY_MOIS,
        MERCH_PAY_jour_semaine,
        poids_MERCH_PAY_jour_mois*poids_MERCH_PAY_jour_semaine poids_MERCH_PAY,
        BILL_PAY_MOIS,
        BILL_PAY_jour_semaine,
        poids_BILL_PAY_jour_mois*poids_BILL_PAY_jour_semaine poids_BILL_PAY
    from junk.poids_kpi_om3
)r


insert into junk.poids_kpi_om
select 
    jour ,
    jour_semaine,
    mois,
    region_administrative,
    region_commerciale,
    REVENU,
    CASH_IN,
    CASH_OUT,
    MERCH_PAY,
    BILL_PAY,
    nb_occ_jour_semaine,
    REVENU_MOIS,
    revenu_jour_semaine,
    REVENU/REVENU_MOIS poids_revenu_jour_mois,
    revenu_jour_semaine/(REVENU_MOIS*nb_occ_jour_semaine) poids_revenu_jour_semaine,
    CASH_IN_MOIS,
    CASH_IN_jour_semaine,
    CASH_IN/CASH_IN_MOIS poids_CASH_IN_jour_mois,
    CASH_IN_jour_semaine/(CASH_IN_MOIS*nb_occ_jour_semaine) poids_CASH_IN_jour_semaine,
    CASH_OUT_MOIS,
    CASH_OUT_jour_semaine,
    CASH_OUT/CASH_OUT_MOIS poids_CASH_OUT_jour_mois,
    CASH_OUT_jour_semaine/(CASH_OUT_MOIS*nb_occ_jour_semaine) poids_CASH_OUT_jour_semaine,
    MERCH_PAY_MOIS,
    MERCH_PAY_jour_semaine,
    MERCH_PAY/MERCH_PAY_MOIS poids_MERCH_PAY_jour_mois,
    MERCH_PAY_jour_semaine/(MERCH_PAY_MOIS*nb_occ_jour_semaine) poids_MERCH_PAY_jour_semaine,
    BILL_PAY_MOIS,
    BILL_PAY_jour_semaine,
    BILL_PAY/BILL_PAY_MOIS poids_BILL_PAY_jour_mois,
    BILL_PAY_jour_semaine/(BILL_PAY_MOIS*nb_occ_jour_semaine) poids_BILL_PAY_jour_semaine
    from (
    select
        jour ,
        jour_semaine,
        mois,
        region_administrative,
        region_commerciale,
        REVENU,
        CASH_IN,
        CASH_OUT,
        MERCH_PAY,
        BILL_PAY,
        nb_occ_jour_semaine,
        sum(REVENU) over (partition by mois ) REVENU_MOIS,
        sum(REVENU) OVER (PARTITION BY mois,region_administrative,region_commerciale, jour_semaine) revenu_jour_semaine,
        sum(CASH_IN) over (partition by mois ) CASH_IN_MOIS,
        sum(CASH_IN) OVER (PARTITION BY mois,region_administrative,region_commerciale,jour_semaine) CASH_IN_jour_semaine,
        sum(CASH_OUT) over (partition by mois ) CASH_OUT_MOIS,
        sum(CASH_OUT) OVER (PARTITION BY mois,region_administrative,region_commerciale,jour_semaine) CASH_OUT_jour_semaine,
        sum(MERCH_PAY) over (partition by mois ) MERCH_PAY_MOIS,
        sum(MERCH_PAY) OVER (PARTITION BY mois,region_administrative,region_commerciale,jour_semaine) MERCH_PAY_jour_semaine,
        sum(BILL_PAY) over (partition by mois ) BILL_PAY_MOIS,
        sum(BILL_PAY) OVER (PARTITION BY mois,region_administrative,region_commerciale,jour_semaine) BILL_PAY_jour_semaine
        from (
        SELECT
            rev.JOUR JOUR,
            dates.jour jour_semaine ,
            substring(rev.JOUR,0,7) mois,
            nb_occ_jour_semaine,
            region_administrative,
            region_commerciale,
            SUM(nvl(REVENU,0)) REVENU,
            SUM(CASE WHEN service_type  IN ('CASHIN') then nvl(val,0) ELSE 0 END) CASH_IN ,
            SUM(CASE WHEN service_type  IN ('CASHOUT') then nvl(val,0) ELSE 0 END) CASH_OUT ,
            SUM(CASE WHEN service_type  IN ('MERCHPAY') then nvl(val,0) ELSE 0 END) MERCH_PAY ,
            SUM(CASE WHEN service_type  IN ('BILLPAY') then nvl(val,0) ELSE 0 END) BILL_PAY
        FROM junk.ft_rev_om rev
        LEFT JOIN (
         select
            site_name,
            max(commercial_region) region_commerciale ,
            max(region) region_administrative
          from DIM.SPARK_DT_GSM_CELL_CODE
          group by site_name
        ) site on upper(trim(rev.site_name)) = upper(trim(site.site_name))
        LEFT JOIN (
            select
                datecode,
                yyyy_mm ,
                jour,
                count(distinct datecode,jour) over (partition by yyyy_mm,jour) nb_occ_jour_semaine
            from DIM.DT_DATES where datecode between '2019-01-01' and '2019-12-31'
        ) dates on rev.jour=dates.datecode
        group by
            rev.JOUR ,
            dates.jour  ,
            substring(rev.JOUR,0,7) ,
            nb_occ_jour_semaine,
            region_administrative,
            region_commerciale
    ) T
)T


insert into   junk.poids_kpi_om4
select
     jour,
     a.jour_semaine jour_semaine,
     substring(jour,0,7) mois ,
     a.region_administrative region_administrative,
     a.region_commerciale region_commerciale,
     revenu,
     cash_in,
     cash_out,
     merch_pay,
     bill_pay,
     nb_occ_jour_semaine,
     REVENU_MOIS,
     poids_revenu_jour_mois,
     cash_in_mois,
     poids_cash_in_jour_mois,
     cash_out_mois,
     poids_cash_out_jour_mois,
     merch_pay_mois,
     poids_merch_pay_jour_mois,
     bill_pay_mois,
     poids_bill_pay_jour_mois, 
    revenu_jour_semaine,
    poids_revenu_jour_semaine,
    CASH_IN_jour_semaine,
    poids_CASH_IN_jour_semaine,
    CASH_OUT_jour_semaine,
    poids_CASH_OUT_jour_semaine,
    MERCH_PAY_jour_semaine,
    poids_MERCH_PAY_jour_semaine,
    BILL_PAY_jour_semaine,
    poids_BILL_PAY_jour_semaine
from (
     select date_sub(datecode,2) jour,b.jour_semaine jour_semaine, substring(datecode,0,7) mois ,region_administrative,region_commerciale,revenu,cash_in,cash_out,merch_pay,bill_pay,nb_occ_jour_semaine,REVENU_MOIS,poids_revenu_jour_mois,cash_in_mois,poids_cash_in_jour_mois,cash_out_mois,poids_cash_out_jour_mois,merch_pay_mois,poids_merch_pay_jour_mois,bill_pay_mois,poids_bill_pay_jour_mois
      from (
        select * from junk.poids_kpi_om where jour>='2019-03-01'
        union
        select date_add(jour,2) jour,jour_semaine,mois,region_administrative,region_commerciale,revenu,cash_in,cash_out,merch_pay,bill_pay,nb_occ_jour_semaine,revenu_mois,revenu_jour_semaine,poids_revenu_jour_mois,poids_revenu_jour_semaine,cash_in_mois,cash_in_jour_semaine,poids_cash_in_jour_mois,poids_cash_in_jour_semaine,cash_out_mois,cash_out_jour_semaine,poids_cash_out_jour_mois,poids_cash_out_jour_semaine,merch_pay_mois,merch_pay_jour_semaine,poids_merch_pay_jour_mois,poids_merch_pay_jour_semaine,bill_pay_mois,bill_pay_jour_semaine,poids_bill_pay_jour_mois,poids_bill_pay_jour_semaine
     from junk.poids_kpi_om where jour>='2019-12-30'
    ) a
     left join (
        select datecode,jour jour_semaine from dim.dt_dates where datecode between '2020-03-01' and '2021-01-03'
    ) b on substring(b.datecode,6,5) =substring(a.jour,6,5)
) a left join (
    select 
        substring(jour,6,2) mois,
        jour_semaine,
        region_administrative,
        region_commerciale,
        max(revenu_jour_semaine) revenu_jour_semaine,
        max(poids_revenu_jour_semaine) poids_revenu_jour_semaine,
        max(CASH_IN_jour_semaine) CASH_IN_jour_semaine,
        max(poids_CASH_IN_jour_semaine) poids_CASH_IN_jour_semaine,
        max(CASH_OUT_jour_semaine) CASH_OUT_jour_semaine,
        max(poids_CASH_OUT_jour_semaine) poids_CASH_OUT_jour_semaine,
        max(MERCH_PAY_jour_semaine) MERCH_PAY_jour_semaine,
        max(poids_MERCH_PAY_jour_semaine) poids_MERCH_PAY_jour_semaine,
        max(BILL_PAY_jour_semaine) BILL_PAY_jour_semaine,
        max(poids_BILL_PAY_jour_semaine) poids_BILL_PAY_jour_semaine 
    from junk.poids_kpi_om where jour>='2019-03-01'
    group by
    substring(jour,6,2),
    jour_semaine,
    region_administrative,
    region_commerciale
) b on substring(a.jour,6,2) =b.mois and a.jour_semaine=b.jour_semaine and nvl(a.region_administrative,'INCONNU') =nvl(b.region_administrative,'INCONNU')  and nvl(a.region_commerciale,'INCONNU') =nvl(b.region_commerciale,'INCONNU')




create external table cdr.BUGGET_PARC_MENSUEL4 (
mois string,
month string,
budget string
)
COMMENT 'external tables-TT'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ';'
LOCATION '/DATALAB/TT/BUGGET_PARC_MENSUEL'
TBLPROPERTIES ('serialization.null.format'='')

create external table cdr.TT_BUDGET_KPI_DD (
month STRING ,
MOIS STRING,
RECHARGE STRING,
reseau_distribution_per STRING ,
self_refill_per STRING ,
scratch_card_per STRING,
acquisitions_en_k STRING ,
SIM_PoS_Actif STRING ,
OM_PoS STRING ,
Airtime_PoS_Actif STRING ,
niveau_stock_reseau_en_MF STRING,
airtime_distributor_level_en_jours STRING,
airtime_retailer_level_en_jours STRING,
region_administrative STRING
)
COMMENT 'external tables-TT'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ';'
LOCATION '/DATALAB/TT/BUDGET_DD'
TBLPROPERTIES ('serialization.null.format'='')


CREATE TABLE  junk.SPLIT_JOUR_KPIS_PARC as
select
A.jour jour,
A.region_administrative region_administrative,
A.region_commerciale region_commerciale,
B.budget budget_mois_parc,
A.parc_mois parc_mois,
poids_parc_jour_mois,
budget*(1-poids_parc_jour_mois)  budget_jour_parc
FROM
(select
*
from  junk.poids_kpi_parc_final) A
left join
(
    select cast(trim(mois) as int) mois , cast ( replace(budget,' ','') as int)  budget  from cdr.BUGGET_PARC_MENSUEL4
) B
ON(substr(A.jour,6,2)=B.mois)
WHERE B.mois IS NOT NULL

insert into  junk.poids_kpi_parc_final
select
     jour,
     a.jour_semaine jour_semaine,
     substring(jour,0,7) mois ,
     a.region_administrative region_administrative,
     a.region_commerciale region_commerciale,
     parc,
     nb_occ_jour_semaine,
     parc_MOIS,
     poids_parc_jour_mois
from (
     select date_sub(datecode,2) jour,b.jour_semaine jour_semaine,nb_occ_jour_semaine, substring(datecode,0,7) mois ,region_administrative,parc_MOIS,region_commerciale,parc,poids_parc_jour_mois
      from (
        select * from junk.poids_kpi_parc2 where jour>='2019-03-01'
    ) a
     left join (
        select datecode,jour jour_semaine from dim.dt_dates where datecode between '2020-03-01' and '2020-12-31'
    ) b on substring(b.datecode,6,5) =substring(a.jour,6,5)
) a


insert into junk.poids_kpi_parc2
select
    jour ,
    jour_semaine,
    mois,
    region_administrative,
    region_commerciale,
    PARC,
    nb_occ_jour_semaine,
    PARC_MOIS,
    PARC_jour_semaine,
    (PARC_MOIS-PARC)/PARC_MOIS poids_PARC_jour_mois,
    (PARC_MOIS-PARC_jour_semaine)/PARC_MOIS poids_PARC_jour_semaine

from (
    select
        jour ,
        jour_semaine,
        mois,
        region_administrative,
        region_commerciale,
        PARC,
        nb_occ_jour_semaine,
        first_value(parc_jour) over (partition by mois order by jour desc) PARC_MOIS,
        PARC_jour_semaine
    from  (
        select
            jour ,
            jour_semaine,
            mois,
            region_administrative,
            region_commerciale,
            PARC,
            nb_occ_jour_semaine,
            sum(parc) over (partition by jour) parc_jour ,
            avg(PARC) OVER (PARTITION BY mois,region_administrative,region_commerciale, jour_semaine ) PARC_jour_semaine
            from (
            SELECT
                to_date(parc.event_date) JOUR,
                dates.jour jour_semaine ,
                substring(parc.event_date,0,7) mois,
                nb_occ_jour_semaine,
                region_administrative,
                region_commerciale,
                SUM(nvl(effectif,0)) PARC
            FROM (select * from ( select event_date,parc_type,statut,administrative_region,effectif, sum(effectif) over (partition by event_date) effectif_jour from backup_dwh.kpis_parc where parc_type='PARC_GROUPE' and statut='ACTIF')T where effectif_jour>5000000) parc
            LEFT JOIN (
             select
                region region_administrative, commercial_region region_commerciale
              from DIM.SPARK_DT_GSM_CELL_CODE
              group by region,commercial_region
            ) site on upper(trim(nvl(parc.administrative_region,'INCONNU'))) = upper(trim(nvl(site.region_administrative,'INCONNU')))
            LEFT JOIN (
                select
                    datecode,
                    yyyy_mm ,
                    jour,
                    count(distinct datecode,jour) over (partition by yyyy_mm,jour) nb_occ_jour_semaine
                from DIM.DT_DATES where datecode between '2018-01-01' and '2019-12-31'
            ) dates on to_date(parc.event_date)=dates.datecode
            group by
                to_date(parc.event_date) ,
                dates.jour  ,
                substring(parc.event_date,0,7) ,
                nb_occ_jour_semaine,
                region_administrative,
                region_commerciale
        ) T
    )T
)T


INSERT INTO TMP.SPLIT_FINAL_BUDGET_OM
select
    A.jour jour,
    A.region_administrative region_administrative,
    A.region_commerciale region_commerciale,
    A.budget_cash_in budget_cash_in,
    A.budget_cash_out budget_cash_out,
    A.budget_merch_bill_pay budget_merch_bill_pay,
    A.budget_revenu budget_revenu,
    A.budget_jour_cash_in budget_jour_cash_in,
    A.budget_jour_cash_out budget_jour_cash_out,
    A.budget_jour_merch_bill_pay budget_jour_merch_bill_pay,
    A.budget_jour_revenu budget_jour_revenu,
    case when A.budget_jour_cash_in<>0 THEN  NVL((A.budget_cash_in-B.somme_budget_jour_cash_in)/(B.nb_jour_mois_cash_in*11),0) ELSE 0 END + A.budget_jour_cash_in  budget_jour_cash_in2,
    case when A.budget_jour_cash_out<>0 THEN NVL((A.budget_cash_out-B.somme_budget_jour_cash_out)/(B.nb_jour_mois_cash_out*11),0) ELSE 0 END + A.budget_jour_cash_out  budget_jour_cash_out2,
    case when A.budget_jour_merch_bill_pay<>0 THEN  NVL((A.budget_merch_bill_pay-B.somme_budget_jour_merch_bill_pay*11)/(B.nb_jour_mois_merch_bill_pay),0) ELSE 0 END  + A.budget_jour_merch_bill_pay   budget_jour_merch_bill_pay2,
    case when A.budget_jour_revenu<>0 THEN NVL((A.budget_revenu-B.somme_budget_jour_revenu)/(B.nb_jour_mois_revenu*11),0) ELSE 0 END + A.budget_jour_revenu   budget_jour_revenu2
from junk.PLIT_JOUR_KPIS_OM A
left join (
    select
        t.mois mois,
        region_administrative,
        region_commerciale,
        somme_budget_jour_revenu,
        somme_budget_jour_cash_in,
        somme_budget_jour_cash_out,
        somme_budget_jour_merch_bill_pay,
        nb_jour_mois_revenu,
        nb_jour_mois_cash_in,
        nb_jour_mois_cash_out,
        nb_jour_mois_merch_bill_pay
    from (
        select mois,region_administrative,region_commerciale,max(nb_jour_mois_revenu) nb_jour_mois_revenu,max(nb_jour_mois_cash_in) nb_jour_mois_cash_in,max(nb_jour_mois_cash_out) nb_jour_mois_cash_out , max(nb_jour_mois_merch_bill_pay) nb_jour_mois_merch_bill_pay from (
            select substr(jour,1,7) mois,region_administrative,region_commerciale,count(distinct jour) nb_jour_mois_revenu, null nb_jour_mois_cash_in,null nb_jour_mois_cash_out,null nb_jour_mois_merch_bill_pay  from junk.PLIT_JOUR_KPIS_OM where budget_jour_revenu>0 group by substr(jour,1,7),region_administrative,region_commerciale
            union all
            select substr(jour,1,7) mois,region_administrative,region_commerciale,null nb_jour_mois_revenu, count(distinct jour) nb_jour_mois_cash_in,null nb_jour_mois_cash_out,null nb_jour_mois_merch_bill_pay  from junk.PLIT_JOUR_KPIS_OM where budget_jour_cash_in>0 group by substr(jour,1,7),region_administrative,region_commerciale
            union all
            select substr(jour,1,7) mois,region_administrative,region_commerciale,null nb_jour_mois_revenu, null nb_jour_mois_cash_in,count(distinct jour) nb_jour_mois_cash_out,null nb_jour_mois_merch_bill_pay  from junk.PLIT_JOUR_KPIS_OM where budget_jour_cash_out>0 group by substr(jour,1,7),region_administrative,region_commerciale
            union all
            select substr(jour,1,7) mois,region_administrative,region_commerciale,null nb_jour_mois_revenu, null nb_jour_mois_cash_in,null nb_jour_mois_cash_out,count(distinct jour) nb_jour_mois_merch_bill_pay  from junk.PLIT_JOUR_KPIS_OM where budget_jour_merch_bill_pay>0 group by substr(jour,1,7),region_administrative,region_commerciale
        )t
        group by mois,region_administrative,region_commerciale order by 1,2,3
    )t
    left join (
        select
            DISTINCT
            substr(jour,1,7) MOIS,
            SUM(budget_jour_cash_in) OVER (PARTITION BY substr(jour,1,7)) somme_budget_jour_cash_in,
            SUM(budget_jour_cash_out) OVER (PARTITION BY substr(jour,1,7)) somme_budget_jour_cash_out,
            SUM(budget_jour_merch_bill_pay) OVER (PARTITION BY substr(jour,1,7)) somme_budget_jour_merch_bill_pay,
            SUM(budget_jour_revenu) OVER (PARTITION BY substr(jour,1,7)) somme_budget_jour_revenu
        FROM junk.PLIT_JOUR_KPIS_OM
    )s on t.mois=s.mois
) B
ON (substr(A.jour,1,7) =B.MOIS AND A.region_administrative=B.region_administrative and a.region_commerciale=b.region_commerciale)


select
    t.mois mois,
    region_administrative,
    region_commerciale,
    somme_budget_jour_revenu,
    somme_budget_jour_cash_in,
    somme_budget_jour_cash_out,
    somme_budget_jour_merch_bill_pay,
    nb_jour_mois_revenu,
    nb_jour_mois_cash_in,
    nb_jour_mois_cash_out,
    nb_jour_mois_merch_bill_pay
from (
    select mois,region_administrative,region_commerciale,max(nb_jour_mois_revenu) nb_jour_mois_revenu,max(nb_jour_mois_cash_in) nb_jour_mois_cash_in,max(nb_jour_mois_cash_out) nb_jour_mois_cash_out , max(nb_jour_mois_merch_bill_pay) nb_jour_mois_merch_bill_pay from (
        select substr(jour,1,7) mois,region_administrative,region_commerciale,count(distinct jour) nb_jour_mois_revenu, null nb_jour_mois_cash_in,null nb_jour_mois_cash_out,null nb_jour_mois_merch_bill_pay  from junk.PLIT_JOUR_KPIS_OM where budget_jour_revenu>0 group by substr(jour,1,7),region_administrative,region_commerciale
        union all
        select substr(jour,1,7) mois,region_administrative,region_commerciale,null nb_jour_mois_revenu, count(distinct jour) nb_jour_mois_cash_in,null nb_jour_mois_cash_out,null nb_jour_mois_merch_bill_pay  from junk.PLIT_JOUR_KPIS_OM where budget_jour_cash_in>0 group by substr(jour,1,7),region_administrative,region_commerciale
        union all
        select substr(jour,1,7) mois,region_administrative,region_commerciale,null nb_jour_mois_revenu, null nb_jour_mois_cash_in,count(distinct jour) nb_jour_mois_cash_out,null nb_jour_mois_merch_bill_pay  from junk.PLIT_JOUR_KPIS_OM where budget_jour_cash_out>0 group by substr(jour,1,7),region_administrative,region_commerciale
        union all
        select substr(jour,1,7) mois,region_administrative,region_commerciale,null nb_jour_mois_revenu, null nb_jour_mois_cash_in,null nb_jour_mois_cash_out,count(distinct jour) nb_jour_mois_merch_bill_pay  from junk.PLIT_JOUR_KPIS_OM where budget_jour_merch_bill_pay>0 group by substr(jour,1,7),region_administrative,region_commerciale
    )t
    group by mois,region_administrative,region_commerciale order by 1,2,3
)t
left join (
    select
        DISTINCT
        substr(jour,1,7) MOIS,
        SUM(budget_jour_cash_in) OVER (PARTITION BY substr(jour,1,7)) somme_budget_jour_cash_in,
        SUM(budget_jour_cash_out) OVER (PARTITION BY substr(jour,1,7)) somme_budget_jour_cash_out,
        SUM(budget_jour_merch_bill_pay) OVER (PARTITION BY substr(jour,1,7)) somme_budget_jour_merch_bill_pay,
        SUM(budget_jour_revenu) OVER (PARTITION BY substr(jour,1,7)) somme_budget_jour_revenu
    FROM junk.PLIT_JOUR_KPIS_OM
)s on t.mois=s.mois











------------------------------------------------------- REFILL --------------------------------------------------



INSERT INTO tmp.budget_kpi_dd
select
    month,
    mois,
    case
        when month='2020-12' and upper(region_administrative)='ADAMAOUA' then recharge/1000000
        else recharge
    end recharge,
    reseau_distribution_per,
    self_refill_per,
    scratch_card_per,
    acquisitions,
    SIM_PoS_Actif,
    OM_PoS,
    Airtime_PoS_Actif,
    niveau_stock_reseau,
    airtime_distributor_level_en_jours,
    airtime_retailer_level_en_jours,
    region_administrative
    from (
    select
    '2020-'lpad,
    trim(mois) mois,
    cast( replace(replace(trim(recharge),' ',''),',','.')  as double)*1000000 recharge ,
    cast(replace(replace(trim(reseau_distribution_per),' ',''),',','.') as double) reseau_distribution_per,
    cast(replace(replace(trim(self_refill_per),' ',''),',','.') as double) self_refill_per ,
    cast(replace(replace(trim(scratch_card_per),' ',''),',','.') as double) scratch_card_per ,
    cast(cast(replace(replace(trim(acquisitions_en_k),' ',''),',','.') as double)*1000 as int) acquisitions ,
    cast(replace(replace(trim(SIM_PoS_Actif),' ',''),',','.') as int) SIM_PoS_Actif ,
    cast(replace(replace(trim(OM_PoS),' ',''),',','.') as int) OM_PoS ,
    cast(replace(replace(trim(Airtime_PoS_Actif),' ',''),',','.') as int) Airtime_PoS_Actif ,
    cast(replace(replace(trim(niveau_stock_reseau_en_MF),' ',''),',','.') as double)*1000000  niveau_stock_reseau ,
    cast(replace(replace(trim(airtime_distributor_level_en_jours),' ',''),',','.') as int) airtime_distributor_level_en_jours ,
    cast(replace(replace(trim(airtime_retailer_level_en_jours),' ',''),',','.') as int) airtime_retailer_level_en_jours ,
    trim(region_administrative) region_administrative
    from cdr.TT_BUDGET_KPI_DD
)T


INSERT INTO TMP.SPLIT_FINAL_BUDGET_OM
select
    A.jour jour,
    A.region_administrative region_administrative,
    A.region_commerciale region_commerciale,
    A.budget_cash_in budget_cash_in,
    A.budget_cash_out budget_cash_out,
    A.budget_merch_bill_pay budget_merch_bill_pay,
    A.budget_revenu budget_revenu,
    A.budget_jour_cash_in budget_jour_cash_in,
    A.budget_jour_cash_out budget_jour_cash_out,
    A.budget_jour_merch_bill_pay budget_jour_merch_bill_pay,
    A.budget_jour_revenu budget_jour_revenu,
    case when A.budget_jour_cash_in<>0 THEN  NVL((A.budget_cash_in-B.somme_budget_jour_cash_in)/(B.nb_jour_mois_cash_in*11),0) ELSE 0 END + A.budget_jour_cash_in  budget_jour_cash_in2,
    case when A.budget_jour_cash_out<>0 THEN NVL((A.budget_cash_out-B.somme_budget_jour_cash_out)/(B.nb_jour_mois_cash_out*11),0) ELSE 0 END + A.budget_jour_cash_out  budget_jour_cash_out2,
    case when A.budget_jour_merch_bill_pay<>0 THEN  NVL((A.budget_merch_bill_pay-B.somme_budget_jour_merch_bill_pay*11)/(B.nb_jour_mois_merch_bill_pay),0) ELSE 0 END  + A.budget_jour_merch_bill_pay   budget_jour_merch_bill_pay2,
    case when A.budget_jour_revenu<>0 THEN NVL((A.budget_revenu-B.somme_budget_jour_revenu)/(B.nb_jour_mois_revenu*11),0) ELSE 0 END + A.budget_jour_revenu   budget_jour_revenu2
from junk.PLIT_JOUR_KPIS_OM A
left join (
    select
        t.mois mois,
        region_administrative,
        region_commerciale,
        somme_budget_jour_revenu,
        somme_budget_jour_cash_in,
        somme_budget_jour_cash_out,
        somme_budget_jour_merch_bill_pay,
        nb_jour_mois_revenu,
        nb_jour_mois_cash_in,
        nb_jour_mois_cash_out,
        nb_jour_mois_merch_bill_pay
    from (
        select mois,region_administrative,region_commerciale,max(nb_jour_mois_revenu) nb_jour_mois_revenu,max(nb_jour_mois_cash_in) nb_jour_mois_cash_in,max(nb_jour_mois_cash_out) nb_jour_mois_cash_out , max(nb_jour_mois_merch_bill_pay) nb_jour_mois_merch_bill_pay from (
            select substr(jour,1,7) mois,region_administrative,region_commerciale,count(distinct jour) nb_jour_mois_revenu, null nb_jour_mois_cash_in,null nb_jour_mois_cash_out,null nb_jour_mois_merch_bill_pay  from junk.PLIT_JOUR_KPIS_OM where budget_jour_revenu>0 group by substr(jour,1,7),region_administrative,region_commerciale
            union all
            select substr(jour,1,7) mois,region_administrative,region_commerciale,null nb_jour_mois_revenu, count(distinct jour) nb_jour_mois_cash_in,null nb_jour_mois_cash_out,null nb_jour_mois_merch_bill_pay  from junk.PLIT_JOUR_KPIS_OM where budget_jour_cash_in>0 group by substr(jour,1,7),region_administrative,region_commerciale
            union all
            select substr(jour,1,7) mois,region_administrative,region_commerciale,null nb_jour_mois_revenu, null nb_jour_mois_cash_in,count(distinct jour) nb_jour_mois_cash_out,null nb_jour_mois_merch_bill_pay  from junk.PLIT_JOUR_KPIS_OM where budget_jour_cash_out>0 group by substr(jour,1,7),region_administrative,region_commerciale
            union all
            select substr(jour,1,7) mois,region_administrative,region_commerciale,null nb_jour_mois_revenu, null nb_jour_mois_cash_in,null nb_jour_mois_cash_out,count(distinct jour) nb_jour_mois_merch_bill_pay  from junk.PLIT_JOUR_KPIS_OM where budget_jour_merch_bill_pay>0 group by substr(jour,1,7),region_administrative,region_commerciale
        )t
        group by mois,region_administrative,region_commerciale order by 1,2,3
    )t
    left join (
        select
            DISTINCT
            substr(jour,1,7) MOIS,
            SUM(budget_jour_cash_in) OVER (PARTITION BY substr(jour,1,7)) somme_budget_jour_cash_in,
            SUM(budget_jour_cash_out) OVER (PARTITION BY substr(jour,1,7)) somme_budget_jour_cash_out,
            SUM(budget_jour_merch_bill_pay) OVER (PARTITION BY substr(jour,1,7)) somme_budget_jour_merch_bill_pay,
            SUM(budget_jour_revenu) OVER (PARTITION BY substr(jour,1,7)) somme_budget_jour_revenu
        FROM junk.PLIT_JOUR_KPIS_OM
    )s on t.mois=s.mois
) B
ON (substr(A.jour,1,7) =B.MOIS AND A.region_administrative=B.region_administrative and a.region_commerciale=b.region_commerciale)



------ calcul des poids refill---------------------------

create table junk.poids_kpi_refill as
select
    jour ,
    jour_semaine,
    mois,
    region_administrative,
    region_commerciale,
    REFILL_AMOUNT,
    nb_occ_jour_semaine,
    REFILL_AMOUNT_MOIS,
    REFILL_AMOUNT_jour_semaine,
    REFILL_AMOUNT/REFILL_AMOUNT_MOIS poids_REFILL_AMOUNT_jour_mois,
    REFILL_AMOUNT_jour_semaine/(REFILL_AMOUNT_MOIS*nb_occ_jour_semaine) poids_REFILL_AMOUNT_jour_semaine
    from (
    select
        jour ,
        jour_semaine,
        mois,
        region_administrative,
        region_commerciale,
        REFILL_AMOUNT,
        nb_occ_jour_semaine,
        sum(REFILL_AMOUNT) over (partition by mois ) REFILL_AMOUNT_MOIS,
        sum(REFILL_AMOUNT) OVER (PARTITION BY mois,region_administrative,region_commerciale, jour_semaine) REFILL_AMOUNT_jour_semaine
        from (
        SELECT
            refill.refill_date JOUR,
            dates.jour jour_semaine ,
            substring(refill.refill_date,0,7) mois,
            nb_occ_jour_semaine,
            region_administrative,
            region_commerciale,
            SUM(nvl(REFILL_AMOUNT,0)) REFILL_AMOUNT
        FROM (select to_date(refill_date) refill_date,site_name,sum(refill_amount)refill_amount from `backup_dwh`.`ft_kpi_refill2` where to_date(refill_date)>='2019-01-01' group by to_date(refill_date) ,site_name order by 1) refill
        LEFT JOIN (
         select
            site_name,
            max(commercial_region) region_commerciale ,
            max(region) region_administrative
          from DIM.SPARK_DT_GSM_CELL_CODE
          group by site_name
        ) site on upper(trim(refill.site_name)) = upper(trim(site.site_name))
        LEFT JOIN (
            select
                datecode,
                yyyy_mm ,
                jour,
                count(distinct datecode,jour) over (partition by yyyy_mm,jour) nb_occ_jour_semaine
            from DIM.DT_DATES where datecode between '2019-01-01' and '2019-12-31'
        ) dates on refill.refill_date=dates.datecode
        group by
            refill.refill_date ,
            dates.jour  ,
            substring(refill.refill_date,0,7) ,
            nb_occ_jour_semaine,
            region_administrative,
            region_commerciale
    ) T
)T


------------------- refill avec decallage  de 2 j ----------------------------

create table junk.poids_kpi_refill_decal2j as
select
     jour,
     a.jour_semaine jour_semaine,
     substring(jour,0,7) mois ,
     a.region_administrative region_administrative,
     a.region_commerciale region_commerciale,
     refill_amount,
     nb_occ_jour_semaine,
     refill_amount_MOIS,
     poids_refill_amount_jour_mois
from (
     select date_sub(datecode,2) jour,b.jour_semaine jour_semaine, substring(datecode,0,7) mois ,region_administrative,region_commerciale,refill_amount,nb_occ_jour_semaine,refill_amount_MOIS,poids_refill_amount_jour_mois
      from (
        select * from junk.poids_kpi_refill where jour>='2019-03-01'
    ) a
     left join (
        select datecode,jour jour_semaine from dim.dt_dates where datecode between '2020-03-01' and '2020-12-31'
    ) b on substring(b.datecode,6,5) =substring(a.jour,6,5)
) a left join (
    select
        substring(jour,6,2) mois,
        jour_semaine,
        region_administrative,
        region_commerciale,
        max(refill_amount_jour_semaine) refill_amount_jour_semaine,
        max(poids_refill_amount_jour_semaine) poids_refill_amount_jour_semaine
    from junk.poids_kpi_refill where jour>='2019-03-01'
    group by
    substring(jour,6,2),
    jour_semaine,
    region_administrative,
    region_commerciale
) b on substring(a.jour,6,2) =b.mois and a.jour_semaine=b.jour_semaine and nvl(a.region_administrative,'INCONNU') =nvl(b.region_administrative,'INCONNU')  and nvl(a.region_commerciale,'INCONNU') =nvl(b.region_commerciale,'INCONNU')



--------------------- refill supression de la region innconnu ----------------

create table junk.poids_kpi_refill_decal2j_equilibre2 as
select
    jour,
    jour_semaine,
    mois,
    region_administrative,
    region_commerciale,
    refill_amount+(total_refill_amount_11_reg-total_refill_amount_10_reg)*(refill_amount/total_refill_amount_10_reg) refill_amount,
    nb_occ_jour_semaine,
    refill_amount_mois,
    poids_refill_amount_jour_mois+(total_poids_refill_amount_jour_mois_11_reg-total_poids_refill_amount_jour_mois_10_reg)*(poids_refill_amount_jour_mois/total_poids_refill_amount_jour_mois_10_reg) poids_refill_amount_jour_mois,
    total_refill_amount_10_reg,
    total_refill_amount_11_reg,
    total_poids_refill_amount_jour_mois_10_reg,
    total_poids_refill_amount_jour_mois_11_reg,
    refill_amount/total_refill_amount_10_reg poids_reg
from (
 select
    jour,
    jour_semaine,
    mois,
    region_administrative,
    region_commerciale,
    refill_amount,
    nb_occ_jour_semaine,
    refill_amount_mois,
    poids_refill_amount_jour_mois,
    sum(case when region_administrative is not null then refill_amount else 0 end) over(partition by jour ) total_refill_amount_10_reg,
    sum(refill_amount) over(partition by jour ) total_refill_amount_11_reg ,
    sum(case when region_administrative is not null then poids_refill_amount_jour_mois else 0 end) over(partition by jour ) total_poids_refill_amount_jour_mois_10_reg,
    sum(poids_refill_amount_jour_mois) over(partition by jour ) total_poids_refill_amount_jour_mois_11_reg
from  junk.poids_kpi_refill_decal2j
)T where region_administrative is not null


---------------------------- refill : croisement avec les budgets --------------------------
CREATE TABLE junk.SPLIT_JOUR_KPIS_REFILL AS
select
A.jour jour,
B.region_administrative region_administrative,
B.region_commerciale region_commerciale,
B.budget_recharge budget_recharge,
B.budget_recharge*A.poids_refill_amount_jour_mois  budget_JOUR_recharge
FROM
(
    select jour, sum(poids_refill_amount_jour_mois) poids_refill_amount_jour_mois from junk.poids_kpi_refill_decal2j_equilibre2 group by jour
) A
left join
(select
    month,
    recharge  budget_recharge,
    a.region_administrative region_administrative,
    region_commerciale
    from  tmp.budget_kpi_dd  A
    LEFT JOIN (
     select
        region region_administrative, commercial_region region_commerciale
      from DIM.SPARK_DT_GSM_CELL_CODE
      group by region,commercial_region
    ) site on upper(trim(nvl(A.region_administrative,'INCONNU'))) = upper(trim(nvl(site.region_administrative,'INCONNU')))
) B
ON(substr(A.jour,6,2)=substring(B.month,6,2))

---------------------------- refill : équilibrage des exces sur le mois -------------------


CREATE TABLE  TMP.SPLIT_FINAL_BUDGET_REFILL AS
select
    A.jour jour,
    A.region_administrative region_administrative,
    A.region_commerciale region_commerciale,
    A.budget_jour_recharge budget_jour_recharge,
    A.budget_recharge budget_recharge_mois_reg,
    case when A.budget_jour_recharge<>0 THEN  NVL((A.budget_recharge- B.budget_mois_recharge_reg)/B.nb_jour_mois_recharge,0) ELSE 0 END + A.budget_jour_recharge  budget_jour_recharge2
from junk.SPLIT_JOUR_KPIS_REFILL A
left join (
    select
        t.mois mois,
        t.region_administrative region_administrative,
        t.region_commerciale region_commerciale,
        budget_mois_recharge_reg,
        nb_jour_mois_recharge,
        poids_mois_reg
    from (
        select substr(jour,1,7) mois,region_administrative,region_commerciale,count(distinct jour) nb_jour_mois_recharge  from junk.SPLIT_JOUR_KPIS_REFILL where budget_jour_recharge>0 group by substr(jour,1,7),region_administrative,region_commerciale
    )t
    left join (
        select
           MOIS,
           region_administrative,
           region_commerciale,
           budget_mois_recharge_reg,
           budget_mois_recharge_reg/(sum(budget_mois_recharge_reg) over (partition by mois) ) poids_mois_reg
        FROM (
            select
                DISTINCT
                substr(jour,1,7) MOIS,
                region_administrative,
                region_commerciale,
                SUM(budget_jour_recharge) OVER (PARTITION BY substr(jour,1,7),region_administrative,region_commerciale) budget_mois_recharge_reg
            FROM junk.SPLIT_JOUR_KPIS_REFILL
        )T
    )s on t.mois=s.mois and  upper(trim(nvl(s.region_administrative,'INCONNU'))) = upper(trim(nvl(t.region_administrative,'INCONNU')))
) B
ON (substr(A.jour,1,7) =B.MOIS AND A.region_administrative=B.region_administrative and a.region_commerciale=b.region_commerciale)


create table tmp.dt_services2 as
select
    trim(nvl(b.event,a.event)) event,
    nvl(b.event_source,a.event_source)  event_source,
    nvl(b.service_code,a.service_code)  service_code,
    nvl(b.description,a.description)  description,
    nvl(b.insert_date,a.insert_date)  insert_date,
    nvl(b.family_mkt,a.family_mkt)  family_mkt,
    nvl(b.family_eoy,a.family_eoy)  family_eoy,
    nvl(b.validity,a.validity)  validity,
    nvl(b.usage,a.usage)  usage,
    nvl(b.flybox_dongle,a.flybox_dongle)  flybox_dongle,
    nvl(b.offres_b_to_b,a.offres_b_to_b)  offres_b_to_b,
    nvl(b.usage_mkt,a.usage_mkt)  usage_mkt,
    nvl(b.produit,a.produit)  produit,
    if(a.event is not null and b.event is not null ,b.voix_onnet,nvl(a.voix_onnet,b.voix_onnet)) voix_onnet,
    if(a.event is not null and b.event is not null ,b.voix_offnet,nvl(a.voix_offnet,b.voix_offnet)) voix_offnet,
    if(a.event is not null and b.event is not null ,b.voix_inter,nvl(a.voix_inter,b.voix_inter)) voix_inter,
    if(a.event is not null and b.event is not null ,b.voix_roaming,nvl(a.voix_roaming,b.voix_roaming)) voix_roaming,
    if(a.event is not null and b.event is not null ,b.sms_onnet,nvl(a.sms_onnet,b.sms_onnet)) sms_onnet,
    if(a.event is not null and b.event is not null ,b.sms_offnet,nvl(a.sms_offnet,b.sms_offnet)) sms_offnet,
    if(a.event is not null and b.event is not null ,b.sms_inter,nvl(a.sms_inter,b.sms_inter)) sms_inter,
    if(a.event is not null and b.event is not null ,b.sms_roaming,nvl(a.sms_roaming,b.sms_roaming)) sms_roaming,
    if(a.event is not null and b.event is not null ,b.data_bundle,nvl(a.data_bundle,b.data_bundle)) data_bundle,
    if(a.event is not null and b.event is not null ,b.sva,nvl(a.sva,b.sva)) sva,
    if(a.event is not null and b.event is not null ,b.validite,nvl(a.validite,b.validite)) validite,
    nvl(b.marche,a.marche)  marche,
    nvl(b.segment,a.segment)  segment,
    nvl(b.family_mktsubs,a.family_mktsubs)  family_mktsubs,
    nvl(b.family_magicbundles,a.family_magicbundles)  family_magicbundles,
    if(a.event is not null and b.event is not null ,b.prix,nvl(a.prix,b.prix)) prix,
    nvl(b.type_ocm,a.type_ocm)  type_ocm,
    nvl(b.combo,a.combo)  combo
from (select * from (select a.*, row_number() over (partition by upper(trim(event)) order by insert_date desc )rg from DIM.dt_services a) a where rg=1) a
full outer join (
    SELECT
        event,
        NULL event_source,
        NULL service_code,
        NULL description,
        CURRENT_TIMESTAMP insert_date,
        NULL family_mkt,
        NULL family_eoy,
        NULL validity,
        NULL usage,
        NULL flybox_dongle,
        NULL offres_b_to_b,
        NULL usage_mkt,
        NULL produit,
        voix_onnet,
        voix_offnet,
        voix_inter,
        voix_roaming,
        sms_onnet,
        sms_offnet,
        sms_inter,
        sms_roaming,
        data_bundle,
        sva sva,
        validite,
        NULL marche,
        NULL segment,
        NULL family_mktsubs,
        NULL family_magicbundles,
        prix,
        type_ocm,
        combo
    from (
         select
            bdle_name event,
            prix,
            coeff_onnet/100 voix_onnet,
            coeff_offnet /100 voix_offnet,
            coeff_inter/100  voix_inter,
            coeff_roaming_voix/100  voix_roaming,
            coef_sms/100 sms_onnet,
            null sms_offnet,
            null sms_inter,
            coef_roaming_sms/100 sms_roaming,
            (nvl(coeff_data,0)+nvl(coeff_roaming_data,0))/100 data_bundle ,
            coef_sva/100 sva ,
            combo ,
            type_ocm ,
            validite ,
            row_number() over(partition by trim(upper(bdle_name)) order by coeff_inter desc )  rg
        from CDR.TT_REF_SUBS2
    )REF where rg=1 and event is not null
) b on lower(trim(a.event))=lower(trim(b.event))

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


INSERT INTO tmp.SPARK_IT_OMNY_TRANSACTIONS
SELECT
SENDER_MSISDN,
RECEIVER_MSISDN,
RECEIVER_USER_ID,
SENDER_USER_ID,
TRANSACTION_AMOUNT,
COMMISSIONS_PAID,
COMMISSIONS_RECEIVED,
COMMISSIONS_OTHERS,
SERVICE_CHARGE_RECEIVED,
SERVICE_CHARGE_PAID,
TAXES,
SERVICE_TYPE,
TRANSFER_STATUS,
SENDER_PRE_BAL,
SENDER_POST_BAL,
RECEIVER_PRE_BAL,
RECEIVER_POST_BAL,
SENDER_ACC_STATUS,
RECEIVER_ACC_STATUS,
ERROR_CODE,
ERROR_DESC,
REFERENCE_NUMBER,
CREATED_ON,
CREATED_BY,
MODIFIED_ON,
MODIFIED_BY,
APP_1_DATE,
APP_2_DATE,
TRANSFER_ID,
TRANSFER_DATETIME TRANSFER_DATETIME_NQ,
SENDER_CATEGORY_CODE,
SENDER_DOMAIN_CODE,
SENDER_GRADE_NAME,
SENDER_GROUP_ROLE,
SENDER_DESIGNATION,
SENDER_STATE,
RECEIVER_CATEGORY_CODE,
RECEIVER_DOMAIN_CODE,
RECEIVER_GRADE_NAME,
RECEIVER_GROUP_ROLE,
RECEIVER_DESIGNATION,
RECEIVER_STATE,
SENDER_CITY,
RECEIVER_CITY,
APP_1_BY,
APP_2_BY,
REQUEST_SOURCE,
GATEWAY_TYPE,
TRANSFER_SUBTYPE,
PAYMENT_TYPE,
PAYMENT_NUMBER,
PAYMENT_DATE,
REMARKS,
ACTION_TYPE,
TRANSACTION_TAG,
RECONCILIATION_BY,
RECONCILIATION_FOR,
EXT_TXN_NUMBER,
ORIGINAL_REF_NUMBER,
ZEBRA_AMBIGUOUS,
ATTEMPT_STATUS,
OTHER_MSISDN,
SENDER_WALLET_NUMBER,
RECEIVER_WALLET_NUMBER,
SENDER_USER_NAME,
RECEIVER_USER_NAME,
TNO_MSISDN,
TNO_ID,
UNREG_FIRST_NAME,
UNREG_LAST_NAME,
UNREG_DOB,
UNREG_ID_NUMBER,
BULK_PAYOUT_BATCHID,
IS_FINANCIAL,
TRANSFER_DONE,
INITIATOR_MSISDN,
VALIDATOR_MSISDN,
INITIATOR_COMMENTS,
VALIDATOR_COMMENTS,
SENDER_WALLET_NAME,
RECIEVER_WALLET_NAME,
SENDER_USER_TYPE,
RECEIVER_USER_TYPE,
ORIGINAL_FILE_NAME,
NULL ORIGINAL_FILE_SIZE,
NULL ORIGINAL_FILE_LINE_COUNT,
ORIGINAL_FILE_DATE,
INSERT_DATE,
to_date(TRANSFER_DATETIME),
ORIGINAL_FILE_DATE FILE_DATE
from backup_dwh.IT_OMNY_TRANSACTIONS4

create table junk.photo_in_localise2 as
select
CONTRACT_ID,
CUSTOMER_ID,
ACCESS_KEY,
ACCOUNT_ID,
ACTIVATION_DATE,
DEACTIVATION_DATE,
INACTIVITY_BEGIN_DATE,
BLOCKED,
EXHAUSTED,
PERIODIC_FEE,
SCRATCH_RELOAD_SUSP,
COMMERCIAL_OFFER_ASSIGN_DATE,
COMMERCIAL_OFFER,
CURRENT_STATUS,
STATUS_DATE,
LOGIN,
LANG,
LOCATION,
MAIN_IMSI,
MSID_TYPE,
PROFILE,
BAD_RELOAD_ATTEMPTS,
LAST_TOPUP_DATE,
LAST_CREDIT_UPDATE_DATE,
BAD_PIN_ATTEMPTS,
BAD_PWD_ATTEMPTS,
OSP_ACCOUNT_TYPE,
INACTIVITY_CREDIT_LOSS,
DEALER_ID,
PROVISIONING_DATE,
MAIN_CREDIT,
PROMO_CREDIT,
SMS_CREDIT,
DATA_CREDIT,
USED_CREDIT_MONTH,
USED_CREDIT_LIFE,
BUNDLE_LIST,
BUNDLE_UNIT_LIST,
PROMO_AND_DISCOUNT_LIST,
INSERT_DATE,
SRC_TABLE,
OSP_STATUS,
BSCS_COMM_OFFER_ID,
BSCS_COMM_OFFER,
INITIAL_SELECTION_DONE,
NOMORE_CREDIT,
PWD_BLOCKED,
FIRST_EVENT_DONE,
CUST_EXT_ID,
CUST_GROUP,
CUST_CATEGORY,
CUST_BILLCYCLE,
CUST_SEGMENT,
OSP_CONTRACT_TYPE,
OSP_CUST_COMMERCIAL_OFFER,
OSP_CUSTOMER_CGLIST,
OSP_CUSTOMER_FORMULE,
BSCS_ACTIVATION_DATE,
BSCS_DEACTIVATION_DATE,
OPERATOR_CODE,
BALANCE_LIST,
PREVIOUS_STATUS,
CURRENT_STATUS_1,
STATE_DATETIME,
CI LOCATION_CI,
a.EVENT_DATE from (select * from mon.spark_ft_contract_snapshot where event_date in ('2020-07-03','2020-07-04','2020-07-05','2020-07-06','2020-07-09','2020-07-10','2020-07-11','2020-07-13','2020-07-16','2020-07-19','2020-07-26')
) a
left join (select msisdn,site_name,event_date from mon.spark_ft_client_last_site_day where event_date in ('2020-07-03','2020-07-04','2020-07-05','2020-07-06','2020-07-09','2020-07-10','2020-07-11','2020-07-13','2020-07-16','2020-07-19','2020-07-26')
    group by msisdn,site_name,event_date
) b on a.event_date=b.event_date and a.ACCESS_KEY=b.msisdn
left join (select upper(site_name) site_name,max(ci) ci from DIM.SPARK_DT_GSM_CELL_CODE group by upper(site_name)) c on upper(b.site_name)=upper(c.site_name)


create table junk.account_act_localise4 as
select
a.MSISDN,
OG_CALL,
IC_CALL_1,
IC_CALL_2,
IC_CALL_3,
IC_CALL_4,
STATUS,
GP_STATUS,
GP_STATUS_DATE,
GP_FIRST_ACTIVE_DATE,
ACTIVATION_DATE,
RESILIATION_DATE,
PROVISION_DATE,
FORMULE,
PLATFORM_STATUS,
REMAIN_CREDIT_MAIN,
REMAIN_CREDIT_PROMO,
LANGUAGE_ACC,
SRC_TABLE,
CONTRACT_ID,
CUSTOMER_ID,
ACCOUNT_ID,
LOGIN,
ICC_COMM_OFFER,
BSCS_COMM_OFFER,
BSCS_STATUS,
OSP_ACCOUNT_TYPE,
CUST_GROUP,
CUST_BILLCYCLE,
BSCS_STATUS_DATE,
INACTIVITY_BEGIN_DATE,
COMGP_STATUS,
COMGP_STATUS_DATE,
COMGP_FIRST_ACTIVE_DATE,
INSERT_DATE,
CI LOCATION_CI,
a.EVENT_DATE
from (select * from mon.spark_ft_account_activity where event_date in ('2020-07-03','2020-07-04','2020-07-05','2020-07-06','2020-07-09','2020-07-10','2020-07-16','2020-07-19')
) a
left join (select msisdn,site_name,event_date from mon.spark_ft_client_last_site_day where event_date in ('2020-07-03','2020-07-04','2020-07-05','2020-07-06','2020-07-09','2020-07-10','2020-07-16','2020-07-19')
    group by msisdn,site_name,event_date
) b on a.event_date=b.event_date and a.MSISDN=b.msisdn
left join (select upper(site_name) site_name,max(ci) ci from DIM.SPARK_DT_GSM_CELL_CODE group by upper(site_name)) c on upper(b.site_name)=upper(c.site_name)


create table junk.kpi_om_tmp as
SELECT
region ADMINISTRATIVE_REGION,
PROFILE PROFILE_CODE,
DETAILS,
sum(VAL) val,
sum(VOL) vol,
sum(REVENU) revenu,
sum(COMMISSION) COMMISSION,
sum(1) NB_LIGNES,
count(distinct msisdn) NB_NUMEROS,
STYLE,
SERVICE_TYPE,
OPERATOR_CODE,
current_timestamp INSERT_DATE,
to_date(JOUR) jour
from backup_dwh.datamart_om_marketing a
left join  (
    select * from mon.spark_ft_contract_snapshot
    where (event_date between '2020-08-17' and '2020-08-25') or (event_date in ('2020-07-20','2020-09-04'))
) b on to_date(a.jour)=b.event_date and a.msisdn=b.access_key
left join (select ci,max(region) region from DIM.SPARK_DT_GSM_CELL_CODE group by ci) c on cast(c.ci as int)=cast(b.location_ci as int)

group by region,PROFILE,DETAILS,STYLE,SERVICE_TYPE,
OPERATOR_CODE,to_date(JOUR)

insert into cdr.spark_it_zebra_master_balance
select
event_time,
channel_user_id,
user_name,
mobile_number,
category,
mobile_number_1,
geographical_domain,
product,
parent_user_name,
owner_user_name,
available_balance,
agent_balance,
original_file_name,
original_file_date,
insert_date,
user_status,
to_change,
modified_on,
original_file_size,
original_file_line_count,
'2020-10-24' from cdr.spark_it_zebra_master_balance where event_date='2020-10-21'

select
a.processing_date processing_date,
a.datecode datecode,
b.processing_date processing_date2,
a.axe_revenu axe_revenu,
a.axe_subscriber axe_subscriber,
a.valeur valeur,
b.valeur valeur2
from
(select * from

(select
    processing_date,
    axe_revenu,
    axe_subscriber,
    sum(valeur_day) valeur
from MON.SPARK_KPIS_REG_FINAL where GRANULARITE_REG='NATIONAL'
group by processing_date ,axe_revenu,    axe_subscriber
order by 1
) a
left join (
    select datecode from dim.dt_dates
) b on a.processing_date between datecode and datecode +6
) a
left join (
    select
    processing_date,
    axe_revenu,
    axe_subscriber,
    sum(valeur_day) valeur
from MON.SPARK_KPIS_REG_FINAL  where GRANULARITE_REG='NATIONAL'
group by processing_date ,axe_revenu,    axe_subscriber
order by 1
)b on a.datecode=b.processing_date and nvl(a.axe_revenu,'nd') =nvl(b.axe_revenu,'nd') and nvl(a.axe_subscriber,'nd')=nvl(b.axe_subscriber,'nd')