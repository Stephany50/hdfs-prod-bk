
 insert into mon.kpi_reg_final3 select
 region_administrative,
 region_commerciale,
 category,
 kpi,
 axe_revenue,
 axe_subscriber,
 axe_regionale,
 valeur,
 lweek,
 2wa wa2,
 3wa wa3,
 4wa wa4,
 mtd,
 lmtd,
 budget,
 budget_mtd,
 mtd_last_year,
 vslweek,
 vs2wa,
 vs3wa,
 vs4wa,
 mtdvslmdt,
 mdtvsbudget,
 mtd_vs_last_year,
 processing_date
 from tmp.kpi_reg_final

