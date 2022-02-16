select
    EVENT_WEEK,
    SITE_NAME,
    TYPE,
    DESTINATION,
    CONTRACT_TYPE,
    NB_APPELS,
    VOLUME_TOTAL,
    REVENU_MAIN_KFCFA_DIVIDED_BY_1000, 
    REVENU_PROMO_KFCFA_DIVIDED_BY_1000
from
(
    SELECT 
    concat(year(date_sub('###SLICE_VALUE###', 1)), '-S', case when length(WEEKOFYEAR(date_sub('###SLICE_VALUE###', 1))) = 1 then concat('0', WEEKOFYEAR(date_sub('###SLICE_VALUE###', 1))) else WEEKOFYEAR(date_sub('###SLICE_VALUE###', 1)) end) EVENT_WEEK, 
    CI, 
    TYPE, 
    DESTINATION, 
    CONTRACT_TYPE, 
    SUM(NB_APPELS) AS NB_APPELS,
    SUM(VOLUME_TOTAL) AS VOLUME_TOTAL, 
    SUM(REVENU_MAIN_KFCFA) AS REVENU_MAIN_KFCFA_DIVIDED_BY_1000, 
    SUM(REVENU_PROMO_KFCFA) AS REVENU_PROMO_KFCFA_DIVIDED_BY_1000
    FROM  AGG.SPARK_FT_A_REVENU_SITE_NURAN
    WHERE EVENT_DATE BETWEEN date_sub('###SLICE_VALUE###', 7) and date_sub('###SLICE_VALUE###', 1)
    GROUP BY concat(year(date_sub('###SLICE_VALUE###', 1)), '-S', case when length(WEEKOFYEAR(date_sub('###SLICE_VALUE###', 1))) = 1 then concat('0', WEEKOFYEAR(date_sub('###SLICE_VALUE###', 1))) else WEEKOFYEAR(date_sub('###SLICE_VALUE###', 1)) end)
    , CI, TYPE, DESTINATION, CONTRACT_TYPE
) A 
left join dim.dt_ci_lac_site_amn vdci 
on LPAD(trim(A.ci), 5, 0) = lpad(trim(vdci.CI), 5, 0) 
