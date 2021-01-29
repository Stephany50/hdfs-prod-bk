     select
            region_administrative,
            region_commerciale,
            'Leviers de croissance'  category,
            'Revenue Data Mobile'  KPI,
            'Revenue Data Mobile'  axe_vue_transversale,
            null axe_subscriber,
            'REVENU DATA' axe_revenu,
            sum(valeur) +sum(valeur)*0.2125 valeur
       from CDR.SPARK_TT_BUDGET_TELCO_2021  where   event_date  between date_sub('###SLICE_VALUE###',20) and date_sub('###SLICE_VALUE###',14)
        group by
               region_administrative,
               region_commerciale