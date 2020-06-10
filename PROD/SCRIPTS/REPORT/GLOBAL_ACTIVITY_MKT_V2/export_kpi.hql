
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
 
 
 
 
SELECT IF(T_4.CHECK_FILE_COUNT > 23 AND T_1.CHECK_FILE_EXIST > 0 AND T_2.CHECK_FILE_ALL_EXIST > 0 AND T_3.MISSING_FILES = 0, 'OK','NOK')
FROM
    (SELECT COUNT(DISTINCT ORIGINAL_FILE_NAME) CHECK_FILE_COUNT FROM CDR.SPARK_IT_ZTE_CHECK_FILE WHERE CDR_DATE = '2020-05-25' AND ORIGINAL_FILE_DATE = '2020-05-25' AND ORIGINAL_FILE_NAME NOT LIKE '%POST%') T_4,
    (SELECT COUNT(*) CHECK_FILE_EXIST FROM CDR.SPARK_IT_ZTE_CHECK_FILE WHERE CDR_DATE = '2020-05-25'
                                                                         AND (CDR_TYPE = 'ZTE_TRANSFER_CDR' OR CDR_TYPE = 'ZTE_RECHARGE_CDR' OR CDR_TYPE = 'ZTE_ADJUSTMENT_CDR') ) T_1,
    (SELECT COUNT(*) CHECK_FILE_ALL_EXIST FROM CDR.SPARK_IT_ZTE_CHECK_FILE_ALL WHERE FILE_DATE = '2020-05-25'
                                                                                 AND (FILE_TYPE = 'ZTE_TRANSFER_CDR' OR FILE_TYPE = 'ZTE_RECHARGE_CDR' OR FILE_TYPE = 'ZTE_ADJUSTMENT_CDR')) T_2,
    (SELECT COUNT(C.FILE_NAME) MISSING_FILES
     FROM
         (
             SELECT
                 A.FILE_NAME
             FROM
                 (
                     SELECT replace(CDR_NAME, 'pla_', '') FILE_NAME FROM CDR.SPARK_IT_ZTE_CHECK_FILE WHERE CDR_DATE = '2020-05-25'
                                                                                                       AND (CDR_TYPE = 'ZTE_TRANSFER_CDR' OR CDR_TYPE = 'ZTE_RECHARGE_CDR' OR CDR_TYPE = 'ZTE_ADJUSTMENT_CDR')
                     UNION
                     SELECT replace(FILE_NAME, 'pla_', '') FILE_NAME FROM CDR.SPARK_IT_ZTE_CHECK_FILE_ALL WHERE FILE_DATE = '2020-05-25'
                                                                                                            AND (FILE_TYPE = 'ZTE_TRANSFER_CDR' OR FILE_TYPE = 'ZTE_RECHARGE_CDR' OR FILE_TYPE = 'ZTE_ADJUSTMENT_CDR')
                 ) A
         )C
     WHERE
         NOT EXISTS
             (
                 SELECT 1 FROM
                     (SELECT ORIGINAL_FILE_NAME FROM CDR.SPARK_IT_ZTE_RECHARGE WHERE pay_date BETWEEN DATE_SUB('2020-05-25',3) AND '2020-05-25' AND TO_DATE(ORIGINAL_FILE_DATE) = '2020-05-25'
                      UNION SELECT ORIGINAL_FILE_NAME FROM CDR.SPARK_IT_ZTE_ADJUSTMENT WHERE create_date BETWEEN DATE_SUB('2020-05-25',3) AND '2020-05-25' AND TO_DATE(ORIGINAL_FILE_DATE) = '2020-05-25'
                      UNION SELECT ORIGINAL_FILE_NAME FROM CDR.SPARK_IT_ZTE_TRANSFER WHERE pay_date BETWEEN DATE_SUB('2020-05-25',3) AND '2020-05-25' AND TO_DATE(ORIGINAL_FILE_DATE) = '2020-05-25'  ) B
                 WHERE B.ORIGINAL_FILE_NAME = C.FILE_NAME
             )
    ) T_3


