INSERT INTO MON.SMS_OM
SELECT
    MSISDN,
    sms,
    CURRENT_TIMESTAMP INSERT_DATE,
    '###SLICE_VALUE###' TRANSACTION_DATE
FROM(
    SELECT *
    FROM  dim.dt_smsnotification_recipient
    WHERE type='SMSREVENUMKT' AND actif='YES'
)A
 LEFT JOIN (
    SELECT
         'En MF '||DATE_FORMAT('###SLICE_VALUE###','dd/MM')
        ||(case when cash_in>0 then '\n' || ' -Cash In '|| cash_in else '' end)
        ||(case when cash_out>0 then '\n' || ' -Cash Out '|| cash_out else '' end)
        ||(case when p2p>0 then '\n' || ' -P2P '|| p2p else '' end)
        ||(case when recharg>0 then '\n' || ' -Recharg '|| recharg else '' end)
        ||(case when march_pay>0 then '\n' || ' -March Pay '|| march_pay else '' end)
        ||(case when revenu_om>0 then '\n' || ' -Rev '|| revenu_om else '' end)
        ||(case when revenu_om_mtd>0 then '\n' || ' -MTD '|| revenu_om_mtd else '' end)
        ||(case when revenu_om_lmtd>0 then '\n' || ' -LMTD '|| revenu_om_lmtd else '' end)
        ||(case when revenu_om_mtd>0 then '\n' || ' -% '|| round((revenu_om_mtd-revenu_om_lmtd)/revenu_om_lmtd*100,1) else '' end) sms
    FROM (
        SELECT
             round(max(cash_in)/1000000,0)cash_in
            , round(max(cash_out)/1000000,0)cash_out
            , round(max(p2p)/1000000,0)p2p
            , round(max(recharg)/1000000,0)recharg
            , round(max(march_pay)/1000000,0)march_pay
            , round(max(revenu_om)/1000000,2)revenu_om
            , round(max(revenu_om_mtd)/1000000,2)revenu_om_mtd
            , round(max(revenu_om_lmtd)/1000000,2)revenu_om_lmtd
        FROM(
            select
                '###SLICE_VALUE###' tdate
                ,sum(case when lower(AMOUNT_TYPE) like '%montant%cash%out%' then amount else 0 end) cash_out
                ,sum(case when lower(AMOUNT_TYPE) like '%montant%cash%in%' then amount else 0 end) cash_in
                ,sum(case when lower(a.SERVICE_GROUP_CODE) like '%chg_p2p%' then amount else 0 end) p2p
                ,sum(case when lower(a.SERVICE_GROUP_CODE) like '%amt_rc%' then amount else 0 end) recharg
                ,sum(case when lower(a.SERVICE_GROUP_CODE) like '%amt_merchpay%' then amount else 0 end) march_pay
                ,sum(case when lower(AMOUNT_TYPE) like '%revenus%' then amount else 0 end) revenu_om
            from MON.FT_OMNY_GLOBAL_ACTIVITY a
            LEFT JOIN dim.DT_OM_FINANCE_DASHBORD_USAGE b ON a.SERVICE_GROUP_CODE = b.SERVICE_GROUP_CODE
            where event_date = '###SLICE_VALUE###'
            group by event_date
        )omny,
        (
            select
                '###SLICE_VALUE###' tdate
                ,sum(case when lower(AMOUNT_TYPE) like '%revenus%' then amount else 0 end) revenu_om_mtd
            from MON.FT_OMNY_GLOBAL_ACTIVITY a
            LEFT JOIN dim.DT_OM_FINANCE_DASHBORD_USAGE b ON a.SERVICE_GROUP_CODE = b.SERVICE_GROUP_CODE
            where event_date between CONCAT(SUBSTRING('###SLICE_VALUE###',0,7),'-','01') and '###SLICE_VALUE###'
        )omny_mtd,
        (
            select
                '###SLICE_VALUE###' tdate
                ,sum(case when lower(AMOUNT_TYPE) like '%revenus%' then amount else 0 end) revenu_om_lmtd
            from MON.FT_OMNY_GLOBAL_ACTIVITY a
            LEFT JOIN dim.DT_OM_FINANCE_DASHBORD_USAGE b ON a.SERVICE_GROUP_CODE = b.SERVICE_GROUP_CODE
            where event_date between add_months(CONCAT(SUBSTRING('###SLICE_VALUE###',0,7),'-','01'),-1)  and add_months('###SLICE_VALUE###',-1)
        )omny_lmtd
    )t
)B