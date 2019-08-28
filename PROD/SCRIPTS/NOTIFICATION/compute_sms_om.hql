SELECT
     'En MF '||DATE_FORMAT(tdate,'dd/MM')||'\n'
    ||(case when cash_in>0 then '\n' || ' -Cash In '|| cash_in else '' end)
    ||(case when cash_out>0 then '\n' || ' -Cash Out '|| cash_out else '' end)
    ||(case when revenu_om>0 then '\n' || ' -Rev '|| revenu_om else '' end)
    ||(case when revenu_om_mtd>0 then '\n' || ' -MTD '|| revenu_om_mtd else '' end)
    ||(case when revenu_om_lmtd>0 then '\n' || ' -LMTD '|| revenu_om_lmtd else '' end)
    ||(case when revenu_om_mtd>0 then '\n' || ' -% '|| (revenu_om_lmtd-revenu_om_mtd)/revenu_om_mtd*100 else '' end)
FROM (
    SELECT
        '2019-08-16' tdate
        , round(max(cash_in)/1000000,0)cash_in
        , round(max(cash_out)/1000000,0)cash_out
        , round(max(revenu_om)/1000000,2)revenu_om
        , round(max(revenu_om)/1000000,2)revenu_om_mtd
        , round(max(revenu_om)/1000000,2)revenu_om_lmtd
    FROM(
        select
            '2019-08-16' tdate
            ,sum(case when lower(AMOUNT_TYPE) like '%montant%cash%out%' then amount else 0 end) cash_out
            ,sum(case when lower(AMOUNT_TYPE) like '%montant%cash%in%' then amount else 0 end) cash_in
            ,sum(case when lower(AMOUNT_TYPE) like '%revenus%' then amount else 0 end) revenu_om
        from MON.FT_OMNY_GLOBAL_ACTIVITY a
        LEFT JOIN dim.DT_OM_FINANCE_DASHBORD_USAGE b ON a.SERVICE_GROUP_CODE = b.SERVICE_GROUP_CODE
        where event_date = '2019-08-16'
        group by event_date
    )omny,
    (
        select
            '2019-08-16' tdate
            ,sum(case when lower(AMOUNT_TYPE) like '%revenus%' then amount else 0 end) revenu_om_mtd
        from MON.FT_OMNY_GLOBAL_ACTIVITY a
        LEFT JOIN dim.DT_OM_FINANCE_DASHBORD_USAGE b ON a.SERVICE_GROUP_CODE = b.SERVICE_GROUP_CODE
        where event_date between CONCAT(SUBSTRING('2019-08-16',0,7),'-','01') and '2019-08-16'
        group by event_date
    )omny_mtd,
    (
        select
            '2019-08-16' tdate
            ,sum(case when lower(AMOUNT_TYPE) like '%revenus%' then amount else 0 end) revenu_om_lmtd
        from MON.FT_OMNY_GLOBAL_ACTIVITY a
        LEFT JOIN dim.DT_OM_FINANCE_DASHBORD_USAGE b ON a.SERVICE_GROUP_CODE = b.SERVICE_GROUP_CODE
        where event_date between add_months(CONCAT(SUBSTRING('2019-08-16',0,7),'-','01'),-1)  and add_months('2019-08-16',-1)
        group by event_date
    )omny_lmtd
)t