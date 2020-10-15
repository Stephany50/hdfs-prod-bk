-- Incoming MTN
select sdate,
     'NI_IN_MTN' usage_code,
    'FT_X_INTERCO_FINAL' source_table,
    sum(duree_appel)/60 kpi,
    CURRENT_TIMESTAMP
from AGG.SPARK_ft_x_interco_final
where sdate ='###SLICE_VALUE###'
AND upper(faisceau) = 'MTN' and type_appel = 'Entrant'
group by sdate



-- OUTGOING MTN
UNION ALL
select sdate,
    'NI_OUT_MTN' usage_code,
    'FT_X_INTERCO_FINAL' source_table,
    sum(duree_appel)/60 kpi,
    CURRENT_TIMESTAMP
from AGG.SPARK_ft_x_interco_final
where sdate ='###SLICE_VALUE###'
AND upper(faisceau) = 'MTN' and type_appel = 'Sortant'
group by sdate




-- NET MTN
UNION ALL
select sdate,
    'NI_NET_MTN' usage_code,
    'FT_X_INTERCO_FINAL' source_table,
    sum(duree_appel)/60 kpi,
    CURRENT_TIMESTAMP
from AGG.SPARK_ft_x_interco_final
where sdate ='###SLICE_VALUE###'
      AND upper(faisceau) = 'MTN'
group by sdate




-- Incoming Nexttel
UNION ALL
select sdate,
     'NI_IN_NEXTTEL' usage_code,
    'FT_X_INTERCO_FINAL' source_table,
    sum(duree_appel)/60 kpi,
    CURRENT_TIMESTAMP
from AGG.SPARK_ft_x_interco_final
where sdate ='###SLICE_VALUE###'
AND upper(faisceau) = 'VIETTEL' and type_appel = 'Entrant'
group by sdate




-- Outgoing Nexttel
UNION ALL
select sdate,
     'NI_OUT_NEXTTEL' usage_code,
    'FT_X_INTERCO_FINAL' source_table,
    sum(duree_appel)/60 kpi,
    CURRENT_TIMESTAMP
from AGG.SPARK_ft_x_interco_final
where sdate ='###SLICE_VALUE###'
AND upper(faisceau) = 'VIETTEL' and type_appel = 'Sortant'
group by sdate




-- Net Nexttel
UNION ALL
select sdate,
    'NI_NET_NEXTTEL' usage_code,
    'FT_X_INTERCO_FINAL' source_table,
    sum(duree_appel)/60 kpi,
    CURRENT_TIMESTAMP
from AGG.SPARK_ft_x_interco_final
where sdate ='###SLICE_VALUE###'
      AND upper(faisceau) = 'VIETTEL'
group by sdate




-- Incoming All
UNION ALL
select sdate,
     'NI_IN_ALL' usage_code,
    'FT_X_INTERCO_FINAL' source_table,
    sum(duree_appel)/60 kpi,
    CURRENT_TIMESTAMP
from AGG.SPARK_ft_x_interco_final
where sdate ='###SLICE_VALUE###'
and TYPE_APPEL = 'Entrant' and upper(faisceau) IN ('VIETTEL', 'MTN', 'CAMTEL NATIONAL')
group by sdate




-- Outgoing All
UNION ALL
select sdate,
     'NI_OUT_ALL' usage_code,
    'FT_X_INTERCO_FINAL' source_table,
    sum(duree_appel)/60 kpi,
    CURRENT_TIMESTAMP
from AGG.SPARK_ft_x_interco_final
where sdate ='###SLICE_VALUE###'
and TYPE_APPEL = 'Sortant' and upper(faisceau) IN ('VIETTEL', 'MTN', 'CAMTEL NATIONAL')
group by sdate




-- Net All
UNION ALL
select sdate,
     'NI_NET_ALL' usage_code,
    'FT_X_INTERCO_FINAL' source_table,
    sum(duree_appel)/60 kpi,
    CURRENT_TIMESTAMP
from AGG.SPARK_ft_x_interco_final
where sdate ='###SLICE_VALUE###'
and upper(faisceau) IN ('VIETTEL', 'MTN', 'CAMTEL NATIONAL')
group by sdate