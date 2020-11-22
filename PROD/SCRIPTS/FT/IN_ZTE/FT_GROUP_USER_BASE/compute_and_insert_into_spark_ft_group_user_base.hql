-- ajouter les données ICC =>  prepaid et hybride :
insert into MON.SPARK_FT_GROUP_USER_BASE
select
    formule
    ,sum( case when GP_STATUS = 'ACTIF' then 1 else 0 end )effectif
    --,least(IC_CALL_4,IC_CALL_3,IC_CALL_2,IC_CALL_1)dd
    ,sum(case when OG_CALL >= DATE_SUB(event_date,31) or least(IC_CALL_4,IC_CALL_3,IC_CALL_2,IC_CALL_1) >= DATE_SUB(event_date,31)
        then 1 else 0 end) all30daysbase
    ,sum(case when OG_CALL >= DATE_SUB(event_date,1) or least(IC_CALL_4,IC_CALL_3,IC_CALL_2,IC_CALL_1) >= DATE_SUB(event_date,1)
        then 1 else 0 end) dailybase
    ,sum(case when GP_STATUS_DATE >= DATE_SUB(event_date,31) and GP_STATUS = 'ACTIF' then 1 else 0 end) all30dayswinback
    ,sum(case when GP_STATUS_DATE >= DATE_SUB(event_date,31) and GP_STATUS = 'INACT' then 1 else 0 end) all30dayslost
    ,sum(case when GP_STATUS = 'INACT' and GP_STATUS_DATE = DATE_SUB(event_date,1) then 1 else 0 end ) churn
    ,0 GrossAdds
    ,CURRENT_TIMESTAMP insert_date
    ,'FT_ACCOUNT_ACTIVITY' src_table
    ,location_ci
    , date_sub(event_date,1) event_date
from MON.SPARK_FT_ACCOUNT_ACTIVITY a
where event_date in ('2020-07-10')
group by event_date,formule,location_ci