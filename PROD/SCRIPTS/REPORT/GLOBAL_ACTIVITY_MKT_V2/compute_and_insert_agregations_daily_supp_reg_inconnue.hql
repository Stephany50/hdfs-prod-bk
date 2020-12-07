INSERT INTO AGG.SPARK_KPIS_DG_TMP_SUPP_REG_INCONNUE

------- redistribution des valeurs de la région inconnu

select
    region_administrative,
    region_commerciale,
    a.category,
    a.KPI,
    a.axe_vue_transversale,
    a.axe_revenu,
    a.axe_subscriber,
    a.granularite,
    a.valeur+(a.valeur/a.total_reg)*b.valeur valeur,
    a.cummulable,
    CURRENT_TIMESTAMP INSERT_DATE,
    to_date('###SLICE_VALUE###') processing_date,
    a.source_table

from (
    ----------- les kpis regionalisé et le total des regions != de inconnu
    SELECT
        region_administrative,
        region_commerciale,
        category,
        KPI,
        axe_vue_transversale,
        axe_revenu,
        axe_subscriber,
        granularite,
        sum(valeur)  valeur,
        cummulable,
        source_table,
        max(total_reg) total_reg
    from (
        select a.*,sum(valeur) over(partition by category,KPI,axe_vue_transversale,axe_revenu,axe_subscriber,cummulable,granularite,source_table ) total_reg from AGG.SPARK_KPIS_DG_TMP a where processing_date ='###SLICE_VALUE###'  and region_administrative <>'INCONNU' and cummulable NOT in ('MOY','WEEKLY')
        )T
    group by
      region_administrative,
        region_commerciale,
        category,
        KPI,
        axe_vue_transversale,
        axe_revenu,
        axe_subscriber,
        cummulable,
        granularite,
        source_table
) a
left join (
    ----- les KPIS dans des régions INCONNUES
    SELECT
        category,
        KPI,
        axe_vue_transversale,
        axe_revenu,
        axe_subscriber,
        granularite,
        sum(valeur)  valeur,
        cummulable,
        source_table
    from AGG.SPARK_KPIS_DG_TMP where processing_date ='###SLICE_VALUE###'  and region_administrative ='INCONNU' and cummulable NOT in ('MOY','WEEKLY')
    group by
        category,
        KPI,
        axe_vue_transversale,
        axe_revenu,
        axe_subscriber,
        cummulable,
        granularite,
        source_table

) b on nvl(a.category,'ND')=nvl(b.category,'ND') and
    nvl(a.KPI,'ND')=nvl(b.KPI,'ND') and
    nvl(a.axe_vue_transversale,'ND')=nvl(b.axe_vue_transversale,'ND') and
    nvl(a.axe_revenu,'ND')=nvl(b.axe_revenu,'ND') and
    nvl(a.axe_subscriber,'ND')=nvl(b.axe_subscriber,'ND') and
    nvl(a.cummulable,'ND')=nvl(b.cummulable,'ND') and
    nvl(a.granularite,'ND')=nvl(b.granularite,'ND') and
    nvl(a.source_table,'ND')=nvl(b.source_table,'ND')

------ LES KPIS qui ne sont pas regionalisés -------------------
union all
SELECT
 *
from AGG.SPARK_KPIS_DG_TMP where processing_date = '###SLICE_VALUE###'  and cummulable in ('MOY','WEEKLY')

