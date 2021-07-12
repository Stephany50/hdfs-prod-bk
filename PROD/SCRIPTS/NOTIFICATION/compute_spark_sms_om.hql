INSERT INTO MON.SPARK_SMS_OM
SELECT
    MSISDN,
    sms,
    CURRENT_TIMESTAMP INSERT_DATE,
    '###SLICE_VALUE###' TRANSACTION_DATE
FROM(
    SELECT *
    FROM  dim.spark_dt_smsnotification_recipient
    WHERE type='SMSREVENUMKT' AND actif='YES'
)A
 LEFT JOIN (
SELECT
CONCAT('En MF ',DATE_FORMAT('###SLICE_VALUE###','dd/MM')
,(case when cash_in>0 then '\n' || ' -Cash In ' || cash_in else '' end)
,(case when cash_out>0 then '\n' || ' -Cash Out '|| cash_out else '' end)
,(case when p2p>0 then '\n' || ' -P2P '|| p2p else '' end)
,(case when recharg>0 then '\n' || ' -Recharg '|| recharg else '' end)
,(case when march_pay>0 then '\n' || ' -March Pay '|| march_pay else '' end)
,(case when Bill_Pay>0 then '\n' || ' -BillPay '|| Bill_pay else '' end)
,(case when Subs_via_om>0 then '\n' || ' -Subs Via OM '|| Subs_via_om else '' end)
,(case when Parc_om_daily>0 then '\n' || ' -Parc(daily) '|| Parc_om_daily else '' end)
,(case when Parc_om_30_jrs>0 then '\n' || ' -Parc(30jrs) '|| Parc_om_30_jrs else '' end)
,(case when revenu_om>0 then '\n' || ' -Rev '|| revenu_om else '' end)
,(case when revenu_om_mtd>0 then '\n' || ' -MTD '|| revenu_om_mtd else '' end)
,(case when revenu_om_lmtd>0 then '\n' || ' -LMTD '|| revenu_om_lmtd else '' end)
,(case when revenu_om_mtd>0 then '\n' || ' -% '|| round((revenu_om_mtd-revenu_om_lmtd)/revenu_om_lmtd*100,1) else '' end)) sms
FROM (
SELECT
round(max(cash_in)/1000000,0)cash_in
,round(max(cash_out)/1000000,0)cash_out
, round(max(p2p)/1000000,0)p2p
, round(max(recharg)/1000000,0)recharg
, round(max(march_pay)/1000000,0)march_pay
, round(max(Bill_pay)/1000000,0)Bill_pay
, round(max(Subs_via_om)/1000000,0)Subs_via_om
, round(max(Parc_om_daily)/1000000,0)Parc_om_daily
, round(max(Parc_om_30_jrs)/1000000,0)Parc_om_30_jrs
, round(max(revenu_om)/1000000,2)revenu_om
, round(max(revenu_om_mtd)/1000000,2)revenu_om_mtd
, round(max(revenu_om_lmtd)/1000000,2)revenu_om_lmtd
 FROM(
select
'###SLICE_VALUE###' TRANSACTION_DATE
,sum(case when upper(KPI) like '%CASH%IN%' then valeur else 0 end) cash_in
,sum(case when upper(KPI) like '%CASH%OUT%' then valeur else 0 end) cash_out
,sum(case when upper(KPI) like '%P2P%ORANGE%MONEY%' then valeur else 0 end) p2p
,sum(case when upper(KPI) like '%RECHARGE%TOP%UP%' then valeur else 0 end) recharg
,sum(case when upper(KPI) like '%PAIEMENT%MARCHANT%' then valeur else 0 end) march_pay
,sum(case when upper(KPI) like '%PAIEMENT%BILL%' then valeur else 0 end) Bill_pay
,sum(case when upper(KPI) like '%SUBSCRIPTION%VIA%OM%' then valeur else 0 end) Subs_via_om
,sum(case when upper(KPI) like '%PARC%OM%DAILY%' then valeur else 0 end) Parc_om_daily
,sum(case when upper(KPI) like '%PARC%OM%30JRS%' then valeur else 0 end) Parc_om_30_jrs
,sum(case when upper(KPI) like '%REVENUE%ORANGE%MONEY%' then valeur else 0 end) revenu_om
from AGG.SPARK_KPIs_OM WHERE TRANSACTION_DATE='###SLICE_VALUE###')omny,
(
select
'###SLICE_VALUE###' TRANSACTION_DATE
,sum(case when upper(KPI) like '%REVENUE%ORANGE%MONEY%' then valeur else 0 end) revenu_om_mtd
from AGG.SPARK_KPIs_OM
where TRANSACTION_DATE between CONCAT(SUBSTRING('###SLICE_VALUE###',0,7),'-','01') and '###SLICE_VALUE###'
)omny_mtd,
(
select
'###SLICE_VALUE###' TRANSACTION_DATE
,sum(case when upper(KPI) like '%REVENUE%ORANGE%MONEY%' then valeur else 0 end) revenu_om_lmtd
from AGG.SPARK_KPIs_OM where TRANSACTION_DATE between add_months(CONCAT(SUBSTRING('###SLICE_VALUE###',0,7),'-','01'),-1)  and add_months('###SLICE_VALUE###',-1)
)omny_lmtd
)t
)B