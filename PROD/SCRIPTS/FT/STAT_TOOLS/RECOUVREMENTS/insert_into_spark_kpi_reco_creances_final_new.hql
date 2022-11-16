insert into AGG.SPARK_KPI_RECO_CREANCES_FINAL
select
    event_month,
    sum(case when (kpi = 'CA') then val else 0 end) CA,
    sum(case when (kpi = 'VALEUR_ENCAISS') then val else 0 end) VALEUR_ENCAISS,
    sum(case when (kpi = 'OBJECT_ENC') then val else 0 end) OBJECT_ENC,
    sum(case when (kpi = 'VALEUR_ENCAISS') then val else 0 end) / sum(case when (kpi = 'OBJECT_ENC') then val else 0 end) TAUX_REC_GLOB,
    sum(case when (kpi = 'TAUX_30J') then val else 0 end) TAUX_30J,
    sum(case when (kpi = 'TAUX_60J') then val else 0 end) TAUX_60J,
    sum(case when (kpi = 'TAUX_90J') then val else 0 end) TAUX_90J,
    sum(case when (kpi = 'TAUX_90J_PLUS') then val else 0 end) TAUX_90J_PLUS,
    sum(case when (kpi = 'CR_RDM_DEB_90j_PLUS') then val else 0 end) + sum(case when (kpi = 'CR_RDM_CLIENTS_DESAC') then val else 0 end) RDM,
    sum(case when (kpi = 'CR_AM_DEB_90j_PLUS') then val else 0 end) + sum(case when (kpi = 'CR_AM_CLIENTS_DESAC') then val else 0 end) AM,
    sum(case when (kpi = 'NB_CLIENT_SUSP') then val else 0 end) NB_CLIENT_SUSP,
    sum(case when (kpi = 'SOLDE_CR_M') then val else 0 end) - sum(case when (kpi = 'SOLDE_CR_M_1') then val else 0 end) EVOL_CR_GLOB,
    current_timestamp() INSERT_DATE
from AGG.SPARK_KPI_RECO_CREANCES
where event_month = '###SLICE_VALUE###'
group by event_month