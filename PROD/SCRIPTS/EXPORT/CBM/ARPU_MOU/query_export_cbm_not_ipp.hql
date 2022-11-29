select
    upper(A.bdle_name) as BDLE_NAME,
    sum(bdle_cost) as BDLE_COST, sum(NBER_PURCHASE) as NBER_PURCHASE, period
from
(
    select
        offer_name bdle_name,
        sum(RECHARGE_AMOUNT) bdle_cost,
        count(*) NBER_PURCHASE,
        sdate as PERIOD
    FROM MON.SPARK_FT_VAS_RETAILLER_IRIS
    where sdate  ='###SLICE_VALUE###' and PRETUPS_STATUSCODE = '200' and upper(offer_type) not in ('TOPUP')
    group by offer_name, sdate
    union all
    select
        upper(bdle_name) as bdle_name,
        sum(nvl(bdle_cost, 0)) as bdle_cost,
        sum(nvl(NBER_PURCHASE, 0)) as NBER_PURCHASE,
        period
    from mon.SPARK_FT_CBM_BUNDLE_SUBS_DAILY
    WHERE PERIOD ='###SLICE_VALUE###' and bdle_name != ' '
    group by upper(bdle_name), period
) A left join
(
    select
        UPPER(TRIM(BDLE_NAME)) BDLE_NAME, nvl(max(coeff_onnet), 0) coeff_onnet
    from  CDR.SPARK_IT_DIM_REF_SUBSCRIPTIONS
    GROUP BY UPPER(TRIM(BDLE_NAME))
) B on UPPER(trim(A.bdle_name))=UPPER(trim(B.bdle_name))
where B.coeff_onnet is null
and nvl(sum(bdle_cost),0) > 0
and nvl(sum(NBER_PURCHASE),0) > 0
GROUP BY A.bdle_name, period