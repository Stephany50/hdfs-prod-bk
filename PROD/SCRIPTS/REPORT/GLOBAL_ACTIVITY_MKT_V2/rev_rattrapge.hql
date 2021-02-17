

select
     null region_administrative,
     null region_commerciale,
     case
    when category='Distribution' then 'd. Distribution' 
    when category='Digital' then 'e. Digital'
    when category='Subscriber overview' then 'b. Subscriber overview'
    when category='Revenue overview' then 'a. Revenue overview'
    when category='Leviers de croissance' then 'c. Leviers de croissance'
    else category 
end category,
     case
    when kpi='Telco (prepayé+hybrid) + OM' then '01. Telco (prepayé+hybrid) + OM'
    when kpi='dont sortant (~recharges)' then '02. dont sortant (~recharges)'
    when kpi='dont Voix' then '03. dont Voix'
    when kpi='Subscriber base' then '01. Subscriber base'
    when kpi='Gross Adds' then '02. Gross Adds'
    when kpi='Churn' then '03. Churn'
    when kpi='Net adds' then '04. Net adds'
    when kpi='Tx users (30jrs) en %' then '05. Tx users (30jrs) en %'
    when kpi='Revenue Data Mobile' then '01. Revenue Data Mobile'
    when kpi='Price Per Megas' then '02. Price Per Megas'
    when kpi='Data users (30jrs, >1Mo)' then '03. Data users (30jrs, >1Mo)'
    when kpi='Tx users data(30jrs) en %' then '04. Tx users data(30jrs) en %'
    when kpi='Revenue Orange Money' then '05. Revenue Orange Money'
    when kpi='Users OM (30jrs)' then '06. Users OM (30jrs)'
    when kpi='Tx users OM(30jrs) en %' then '07. Tx users OM(30jrs) en %'
    when kpi='Cash In Valeur' then '08. Cash In Valeur'
    when kpi='Cash Out Valeur' then '09. Cash Out Valeur'
    when kpi='Payments(Bill, Merch)' then '10. Payments(Bill, Merch)'
    when kpi='Charge base' then '11. Charge base'
    when kpi='Daily base' then '12. Daily base'
    when kpi='Data users (MTD, >1Mo)' then '13. Data users (MTD, >1Mo)'
    when kpi='Data users (7jrs, >1Mo)' then '14. Data users (7jrs, >1Mo)'
    when kpi='Data users (Daily, >1Mo)' then '15. DataData users (Daily, >1Mo)'
    when kpi='Users OM (MTD)' then '16. Users OM (MTD)'
    when kpi='Users OM (7jrs)' then '17. Users OM (7jrs)'
    when kpi='Users OM (Daily)' then '18. Users OM (Daily)'
    when kpi='Self Top UP ratio (%)' then '01. Self Top UP ratio (%)'
    when kpi='Stock total client(OM)' then '02. Stock total client(OM)'
    when kpi='Nombre de Pos Airtime actif(30jrs)' then '03. Nombre de Pos Airtime actif(30jrs)'
    when kpi='Nombre de Pos OM actif(30jrs)' then '04. Nombre de Pos OM actif(30jrs)'
    when kpi='Niveau de stock @ distributor level (nb jour)' then '05. Niveau de stock @ distributor level (nb jour)'
    when kpi='Niveau de stock @ retailer level (nb jour)' then '06. Niveau de stock @ retailer level (nb jour)'
    when kpi='Engagements' then '01. Engagements'
    when kpi='Pourcentage traité en numérique' then '02. Pourcentage traité en numérique'
    when kpi='Pourcentage appel traité par BOT' then '03. Pourcentage appel traité par BOT'
    when kpi='Durée moyenne de réponse numérique' then '04. Durée moyenne de réponse numérique'
    else kpi
end kpi,
case
    when kpi='Telco (prepayé+hybrid) + OM' then 'Revenu Telco (prepayé+hybrid) + Revenu OM'
    when kpi='dont sortant (~recharges)' then 'Somme des recharges des points de vente vers le consommateur final + auto rechargement des clients (OM) + rechargement via CAG'
    when kpi='dont Voix' then 'Revenu de la voix/sms bundle + revenu voix/sms PAY AS YOU GO'
    when kpi='Subscriber base' then 'Le parc group actif sur les 90 derniers jours'
    when kpi='Gross Adds' then 'Les acquisitions de la période'
    when kpi='Churn' then 'Les déconnexions de la période'
    when kpi='Net adds' then 'Net adds (PARC S0-PARC S-1)'
    when kpi='Tx users (30jrs) en %' then 'Le pourcentage du parc ayant trafiqué sur les 30 derniers jours'
    when kpi='Revenue Data Mobile' then 'Revenue Data bundle + Revenu Data PAY AS YOU GO (Roaming) '
    when kpi='Price Per Megas' then 'Revenu data / le traffic data'
    when kpi='Data users (30jrs, >1Mo)' then 'Le nombre de users ayant fait la data sur les 30 derniers jours'
    when kpi='Tx users data(30jrs) en %' then 'Le pourcentage du parc ayant fait la DATA sur les 30 derniers jours'
    when kpi='Revenue Orange Money' then 'Revenue Orange Money'
    when kpi='Users OM (30jrs)' then 'Le nombre de users OM actifs sur les 30 derniers jours'
    when kpi='Tx users OM(30jrs) en %' then 'Le pourcentage du parc ayant fait des opérations OM 30 derniers jours'
    when kpi='Cash In Valeur' then 'Cash In Valeur'
    when kpi='Cash Out Valeur' then 'Cash Out Valeur'

    when kpi='Payments(Bill, Merch)' then 'Payments(Bill, Merch)'
    when kpi='Self Top UP ratio (%)' then 'Auto rechargement du client final (via OM, Tango...) /recharges globales'
    when kpi='Stock total client(OM)' then 'Stock total client(OM)'
    when kpi='Nombre de Pos Airtime actif(30jrs)' then 'Nombre de Pos Airtime actif(30jrs)'
    when kpi='Nombre de Pos OM actif(30jrs)' then 'Nombre de Pos OM actif(30jrs)'
    when kpi='Niveau de stock @ distributor level (nb jour)' then '(Somme des stocks dans les SIM de catégorie A et A'' à un instant donné)/moyenne de vente au consommateur final de l''ensemble du réseau de distribution'
    when kpi='Niveau de stock @ retailer level (nb jour)' then '(Somme des stocks dans les SIM de détail (PO, PPOS, NPOS) à un instant donné)/moyenne de vente au consommateur final de l''ensemble du réseau de distribution'
    when kpi='Engagements' then 'Engagements'
    when kpi='Pourcentage traité en numérique' then 'Pourcentage traité en numérique'
    when kpi='Pourcentage appel traité par BOT' then 'Pourcentage appel traité par BOT'
    when kpi='Durée moyenne de réponse numérique' then 'Durée moyenne de réponse numérique'
    else kpi
end definition,
     axe_vue_transversale,
     axe_revenu,
     axe_subscriber,
     (CASE
        when kpi='Pourcentage appel traité par BOT' then sum(valeur)
        when kpi='Revenue Data Mobile' then sum(valeur)
        when kpi='Tx users data' then avg(valeur)
        when kpi='Niveau de stock @ distributor level (nb jour)' then avg(valeur)
        when kpi='Churn' then sum(valeur)
        when kpi='Net adds' then sum(valeur)
        when kpi='Niveau de stock @ retailer level (nb jour)' then avg(valeur)
        when kpi='Cash In Valeur' then sum(valeur)
        when kpi='Subscriber base' then sum(valeur)
        when kpi='Payments(Bill, Merch)' then sum(valeur)
        when kpi='Tx users (30jrs)' then avg(valeur)
        when kpi='Self Top UP ratio (%)' then avg(valeur)
        when kpi='Users (30jrs, >1Mo)' then sum(valeur)
        when kpi='Durée moyenne de réponse numérique' then sum(valeur)
        when kpi='dont sortant (~recharges)' then sum(valeur)
        when kpi='Engagements' then sum(valeur)
        when kpi='Revenue Orange Money' then sum(valeur)
        when kpi='Nombre de Pos Airtime actif' then sum(valeur)
        when kpi='Telco (prepayé+hybrid) + OM' then sum(valeur)
        when kpi='Cash Out Valeur' then sum(valeur)
        when kpi='Price Per Megas' then avg(valeur)
        when kpi='Gross Adds' then sum(valeur)
        when kpi='dont Voix' then sum(valeur)
        when kpi='Pourcentage traité en numérique' then sum(valeur)
        when kpi='Recharges (C2S, CAG, OM)' then sum(valeur)
        else sum(valeur)
    END) valeur,
    (CASE
        when kpi='Pourcentage appel traité par BOT' then sum(valeur_lweek)
        when kpi='Revenue Data Mobile' then sum(valeur_lweek)
        when kpi='Tx users data' then avg(valeur_lweek)
        when kpi='Niveau de stock @ distributor level (nb jour)' then avg(valeur_lweek)
        when kpi='Churn' then sum(valeur_lweek)
        when kpi='Net adds' then sum(valeur_lweek)
        when kpi='Niveau de stock @ retailer level (nb jour)' then avg(valeur_lweek)
        when kpi='Cash In valeur_lweek' then sum(valeur_lweek)
        when kpi='Subscriber base' then sum(valeur_lweek)
        when kpi='Payments(Bill, Merch)' then sum(valeur_lweek)
        when kpi='Tx users (30jrs)' then avg(valeur_lweek)
        when kpi='Self Top UP ratio (%)' then avg(valeur_lweek)
        when kpi='Users (30jrs, >1Mo)' then sum(valeur_lweek)
        when kpi='Durée moyenne de réponse numérique' then sum(valeur_lweek)
        when kpi='dont sortant (~recharges)' then sum(valeur_lweek)
        when kpi='Engagements' then sum(valeur_lweek)
        when kpi='Revenue Orange Money' then sum(valeur_lweek)
        when kpi='Nombre de Pos Airtime actif' then sum(valeur_lweek)
        when kpi='Telco (prepayé+hybrid) + OM' then sum(valeur_lweek)
        when kpi='Cash Out valeur_lweek' then sum(valeur_lweek)
        when kpi='Price Per Megas' then avg(valeur_lweek)
        when kpi='Gross Adds' then sum(valeur_lweek)
        when kpi='dont Voix' then sum(valeur_lweek)
        when kpi='Pourcentage traité en numérique' then sum(valeur_lweek)
        when kpi='Recharges (C2S, CAG, OM)' then sum(valeur_lweek)
        else sum(valeur_lweek)
    END) lweek,
    (CASE
        when kpi='Pourcentage appel traité par BOT' then sum(v2wa)
        when kpi='Revenue Data Mobile' then sum(v2wa)
        when kpi='Tx users data' then avg(v2wa)
        when kpi='Niveau de stock @ distributor level (nb jour)' then avg(v2wa)
        when kpi='Churn' then sum(v2wa)
        when kpi='Net adds' then sum(v2wa)
        when kpi='Niveau de stock @ retailer level (nb jour)' then avg(v2wa)
        when kpi='Cash In v2wa' then sum(v2wa)
        when kpi='Subscriber base' then sum(v2wa)
        when kpi='Payments(Bill, Merch)' then sum(v2wa)
        when kpi='Tx users (30jrs)' then avg(v2wa)
        when kpi='Self Top UP ratio (%)' then avg(v2wa)
        when kpi='Users (30jrs, >1Mo)' then sum(v2wa)
        when kpi='Durée moyenne de réponse numérique' then sum(v2wa)
        when kpi='dont sortant (~recharges)' then sum(v2wa)
        when kpi='Engagements' then sum(v2wa)
        when kpi='Revenue Orange Money' then sum(v2wa)
        when kpi='Nombre de Pos Airtime actif' then sum(v2wa)
        when kpi='Telco (prepayé+hybrid) + OM' then sum(v2wa)
        when kpi='Cash Out v2wa' then sum(v2wa)
        when kpi='Price Per Megas' then avg(v2wa)
        when kpi='Gross Adds' then sum(v2wa)
        when kpi='dont Voix' then sum(v2wa)
        when kpi='Pourcentage traité en numérique' then sum(v2wa)
        when kpi='Recharges (C2S, CAG, OM)' then sum(v2wa)
        else sum(v2wa)
    END)  wa2,
    (CASE
        when kpi='Pourcentage appel traité par BOT' then sum(v3wa)
        when kpi='Revenue Data Mobile' then sum(v3wa)
        when kpi='Tx users data' then avg(v3wa)
        when kpi='Niveau de stock @ distributor level (nb jour)' then avg(v3wa)
        when kpi='Churn' then sum(v3wa)
        when kpi='Net adds' then sum(v3wa)
        when kpi='Niveau de stock @ retailer level (nb jour)' then avg(v3wa)
        when kpi='Cash In v3wa' then sum(v3wa)
        when kpi='Subscriber base' then sum(v3wa)
        when kpi='Payments(Bill, Merch)' then sum(v3wa)
        when kpi='Tx users (30jrs)' then avg(v3wa)
        when kpi='Self Top UP ratio (%)' then avg(v3wa)
        when kpi='Users (30jrs, >1Mo)' then sum(v3wa)
        when kpi='Durée moyenne de réponse numérique' then sum(v3wa)
        when kpi='dont sortant (~recharges)' then sum(v3wa)
        when kpi='Engagements' then sum(v3wa)
        when kpi='Revenue Orange Money' then sum(v3wa)
        when kpi='Nombre de Pos Airtime actif' then sum(v3wa)
        when kpi='Telco (prepayé+hybrid) + OM' then sum(v3wa)
        when kpi='Cash Out v3wa' then sum(v3wa)
        when kpi='Price Per Megas' then avg(v3wa)
        when kpi='Gross Adds' then sum(v3wa)
        when kpi='dont Voix' then sum(v3wa)
        when kpi='Pourcentage traité en numérique' then sum(v3wa)
        when kpi='Recharges (C2S, CAG, OM)' then sum(v3wa)
        else sum(v3wa)
    END)  wa3,
    (CASE
        when kpi='Pourcentage appel traité par BOT' then sum(v4wa)
        when kpi='Revenue Data Mobile' then sum(v4wa)
        when kpi='Tx users data' then avg(v4wa)
        when kpi='Niveau de stock @ distributor level (nb jour)' then avg(v4wa)
        when kpi='Churn' then sum(v4wa)
        when kpi='Net adds' then sum(v4wa)
        when kpi='Niveau de stock @ retailer level (nb jour)' then avg(v4wa)
        when kpi='Cash In v4wa' then sum(v4wa)
        when kpi='Subscriber base' then sum(v4wa)
        when kpi='Payments(Bill, Merch)' then sum(v4wa)
        when kpi='Tx users (30jrs)' then avg(v4wa)
        when kpi='Self Top UP ratio (%)' then avg(v4wa)
        when kpi='Users (30jrs, >1Mo)' then sum(v4wa)
        when kpi='Durée moyenne de réponse numérique' then sum(v4wa)
        when kpi='dont sortant (~recharges)' then sum(v4wa)
        when kpi='Engagements' then sum(v4wa)
        when kpi='Revenue Orange Money' then sum(v4wa)
        when kpi='Nombre de Pos Airtime actif' then sum(v4wa)
        when kpi='Telco (prepayé+hybrid) + OM' then sum(v4wa)
        when kpi='Cash Out v4wa' then sum(v4wa)
        when kpi='Price Per Megas' then avg(v4wa)
        when kpi='Gross Adds' then sum(v4wa)
        when kpi='dont Voix' then sum(v4wa)
        when kpi='Pourcentage traité en numérique' then sum(v4wa)
        when kpi='Recharges (C2S, CAG, OM)' then sum(v4wa)
        else sum(v4wa)
    END)  wa4,
    (CASE
        when kpi='Pourcentage appel traité par BOT' then sum(valeur_mtd)
        when kpi='Revenue Data Mobile' then sum(valeur_mtd)
        when kpi='Tx users data' then avg(valeur_mtd)
        when kpi='Niveau de stock @ distributor level (nb jour)' then avg(valeur_mtd)
        when kpi='Churn' then sum(valeur_mtd)
        when kpi='Net adds' then sum(valeur_mtd)
        when kpi='Niveau de stock @ retailer level (nb jour)' then avg(valeur_mtd)
        when kpi='Cash In valeur_mtd' then sum(valeur_mtd)
        when kpi='Subscriber base' then sum(valeur_mtd)
        when kpi='Payments(Bill, Merch)' then sum(valeur_mtd)
        when kpi='Tx users (30jrs)' then avg(valeur_mtd)
        when kpi='Self Top UP ratio (%)' then avg(valeur_mtd)
        when kpi='Users (30jrs, >1Mo)' then sum(valeur_mtd)
        when kpi='Durée moyenne de réponse numérique' then sum(valeur_mtd)
        when kpi='dont sortant (~recharges)' then sum(valeur_mtd)
        when kpi='Engagements' then sum(valeur_mtd)
        when kpi='Revenue Orange Money' then sum(valeur_mtd)
        when kpi='Nombre de Pos Airtime actif' then sum(valeur_mtd)
        when kpi='Telco (prepayé+hybrid) + OM' then sum(valeur_mtd)
        when kpi='Cash Out valeur_mtd' then sum(valeur_mtd)
        when kpi='Price Per Megas' then avg(valeur_mtd)
        when kpi='Gross Adds' then sum(valeur_mtd)
        when kpi='dont Voix' then sum(valeur_mtd)
        when kpi='Pourcentage traité en numérique' then sum(valeur_mtd)
        when kpi='Recharges (C2S, CAG, OM)' then sum(valeur_mtd)
        else sum(valeur_mtd)
    END) valeur_mtd,
    (CASE
        when kpi='Pourcentage appel traité par BOT' then sum(valeur_lmtd)
        when kpi='Revenue Data Mobile' then sum(valeur_lmtd)
        when kpi='Tx users data' then avg(valeur_lmtd)
        when kpi='Niveau de stock @ distributor level (nb jour)' then avg(valeur_lmtd)
        when kpi='Churn' then sum(valeur_lmtd)
        when kpi='Net adds' then sum(valeur_lmtd)
        when kpi='Niveau de stock @ retailer level (nb jour)' then avg(valeur_lmtd)
        when kpi='Cash In valeur_lmtd' then sum(valeur_lmtd)
        when kpi='Subscriber base' then sum(valeur_lmtd)
        when kpi='Payments(Bill, Merch)' then sum(valeur_lmtd)
        when kpi='Tx users (30jrs)' then avg(valeur_lmtd)
        when kpi='Self Top UP ratio (%)' then avg(valeur_lmtd)
        when kpi='Users (30jrs, >1Mo)' then sum(valeur_lmtd)
        when kpi='Durée moyenne de réponse numérique' then sum(valeur_lmtd)
        when kpi='dont sortant (~recharges)' then sum(valeur_lmtd)
        when kpi='Engagements' then sum(valeur_lmtd)
        when kpi='Revenue Orange Money' then sum(valeur_lmtd)
        when kpi='Nombre de Pos Airtime actif' then sum(valeur_lmtd)
        when kpi='Telco (prepayé+hybrid) + OM' then sum(valeur_lmtd)
        when kpi='Cash Out valeur_lmtd' then sum(valeur_lmtd)
        when kpi='Price Per Megas' then avg(valeur_lmtd)
        when kpi='Gross Adds' then sum(valeur_lmtd)
        when kpi='dont Voix' then sum(valeur_lmtd)
        when kpi='Pourcentage traité en numérique' then sum(valeur_lmtd)
        when kpi='Recharges (C2S, CAG, OM)' then sum(valeur_lmtd)
        else sum(valeur_lmtd)
    END) valeur_lmtd,
    (CASE
        when kpi='Pourcentage appel traité par BOT' then sum(valeur_lweek)
        when kpi='Revenue Data Mobile' then sum(valeur_lweek)
        when kpi='Tx users data' then avg(valeur_lweek)
        when kpi='Niveau de stock @ distributor level (nb jour)' then avg(valeur_lweek)
        when kpi='Churn' then sum(valeur_lweek)
        when kpi='Net adds' then sum(valeur_lweek)
        when kpi='Niveau de stock @ retailer level (nb jour)' then avg(valeur_lweek)
        when kpi='Cash In valeur_lweek' then sum(valeur_lweek)
        when kpi='Subscriber base' then sum(valeur_lweek)
        when kpi='Payments(Bill, Merch)' then sum(valeur_lweek)
        when kpi='Tx users (30jrs)' then avg(valeur_lweek)
        when kpi='Self Top UP ratio (%)' then avg(valeur_lweek)
        when kpi='Users (30jrs, >1Mo)' then sum(valeur_lweek)
        when kpi='Durée moyenne de réponse numérique' then sum(valeur_lweek)
        when kpi='dont sortant (~recharges)' then sum(valeur_lweek)
        when kpi='Engagements' then sum(valeur_lweek)
        when kpi='Revenue Orange Money' then sum(valeur_lweek)
        when kpi='Nombre de Pos Airtime actif' then sum(valeur_lweek)
        when kpi='Telco (prepayé+hybrid) + OM' then sum(valeur_lweek)
        when kpi='Cash Out valeur_lweek' then sum(valeur_lweek)
        when kpi='Price Per Megas' then avg(valeur_lweek)
        when kpi='Gross Adds' then sum(valeur_lweek)
        when kpi='dont Voix' then sum(valeur_lweek)
        when kpi='Pourcentage traité en numérique' then sum(valeur_lweek)
        when kpi='Recharges (C2S, CAG, OM)' then sum(valeur_lweek)
        else sum(valeur_lweek)
    END) budget,
    (CASE
        when kpi='Pourcentage appel traité par BOT' then sum(valeur_budget_mtd)
        when kpi='Revenue Data Mobile' then sum(valeur_budget_mtd)
        when kpi='Tx users data' then avg(valeur_budget_mtd)
        when kpi='Niveau de stock @ distributor level (nb jour)' then avg(valeur_budget_mtd)
        when kpi='Churn' then sum(valeur_budget_mtd)
        when kpi='Net adds' then sum(valeur_budget_mtd)
        when kpi='Niveau de stock @ retailer level (nb jour)' then avg(valeur_budget_mtd)
        when kpi='Cash In valeur_budget_mtd' then sum(valeur_budget_mtd)
        when kpi='Subscriber base' then sum(valeur_budget_mtd)
        when kpi='Payments(Bill, Merch)' then sum(valeur_budget_mtd)
        when kpi='Tx users (30jrs)' then avg(valeur_budget_mtd)
        when kpi='Self Top UP ratio (%)' then avg(valeur_budget_mtd)
        when kpi='Users (30jrs, >1Mo)' then sum(valeur_budget_mtd)
        when kpi='Durée moyenne de réponse numérique' then sum(valeur_budget_mtd)
        when kpi='dont sortant (~recharges)' then sum(valeur_budget_mtd)
        when kpi='Engagements' then sum(valeur_budget_mtd)
        when kpi='Revenue Orange Money' then sum(valeur_budget_mtd)
        when kpi='Nombre de Pos Airtime actif' then sum(valeur_budget_mtd)
        when kpi='Telco (prepayé+hybrid) + OM' then sum(valeur_budget_mtd)
        when kpi='Cash Out valeur_budget_mtd' then sum(valeur_budget_mtd)
        when kpi='Price Per Megas' then avg(valeur_budget_mtd)
        when kpi='Gross Adds' then sum(valeur_budget_mtd)
        when kpi='dont Voix' then sum(valeur_budget_mtd)
        when kpi='Pourcentage traité en numérique' then sum(valeur_budget_mtd)
        when kpi='Recharges (C2S, CAG, OM)' then sum(valeur_budget_mtd)
        else sum(valeur_budget_mtd)
    END)   budget_mtd,
    (CASE
        when kpi='Pourcentage appel traité par BOT' then sum(valeur_mtd_last_year)
        when kpi='Revenue Data Mobile' then sum(valeur_mtd_last_year)
        when kpi='Tx users data' then avg(valeur_mtd_last_year)
        when kpi='Niveau de stock @ distributor level (nb jour)' then avg(valeur_mtd_last_year)
        when kpi='Churn' then sum(valeur_mtd_last_year)
        when kpi='Net adds' then sum(valeur_mtd_last_year)
        when kpi='Niveau de stock @ retailer level (nb jour)' then avg(valeur_mtd_last_year)
        when kpi='Cash In valeur_mtd_last_year' then sum(valeur_mtd_last_year)
        when kpi='Subscriber base' then sum(valeur_mtd_last_year)
        when kpi='Payments(Bill, Merch)' then sum(valeur_mtd_last_year)
        when kpi='Tx users (30jrs)' then avg(valeur_mtd_last_year)
        when kpi='Self Top UP ratio (%)' then avg(valeur_mtd_last_year)
        when kpi='Users (30jrs, >1Mo)' then sum(valeur_mtd_last_year)
        when kpi='Durée moyenne de réponse numérique' then sum(valeur_mtd_last_year)
        when kpi='dont sortant (~recharges)' then sum(valeur_mtd_last_year)
        when kpi='Engagements' then sum(valeur_mtd_last_year)
        when kpi='Revenue Orange Money' then sum(valeur_mtd_last_year)
        when kpi='Nombre de Pos Airtime actif' then sum(valeur_mtd_last_year)
        when kpi='Telco (prepayé+hybrid) + OM' then sum(valeur_mtd_last_year)
        when kpi='Cash Out valeur_mtd_last_year' then sum(valeur_mtd_last_year)
        when kpi='Price Per Megas' then avg(valeur_mtd_last_year)
        when kpi='Gross Adds' then sum(valeur_mtd_last_year)
        when kpi='dont Voix' then sum(valeur_mtd_last_year)
        when kpi='Pourcentage traité en numérique' then sum(valeur_mtd_last_year)
        when kpi='Recharges (C2S, CAG, OM)' then sum(valeur_mtd_last_year)
        else sum(valeur_mtd_last_year)
    END)  mtd_last_year,
    avg(vslweek/100) vslweek,
    avg(vs2wa/100) vs2wa,
    avg(vs3wa/100) vs3wa,
    avg(vs4wa/100) vs4wa,
    avg(mtdvslmdt/100) mtdvslmdt,
    avg(mdtvsbudget/100) mdtvsbudget,
    avg(mtd_vs_last_year/100) mtd_vs_last_year ,
     processing_date
     from (
          
select 
    region_administrative,
    region_commerciale,
    category,
     kpi,
    axe_vue_transversale,
    axe_revenu,
    axe_subscriber,
    source_table,
    (case when axe_revenu in ('REVENU DATA','REVENU VOIX SORTANT','REVENUE TELCO (Prepaid+Hybrid+OM)','REVENU OM') then valeur/1.1925/1.02 else valeur end) valeur,
    (case when axe_revenu in ('REVENU DATA','REVENU VOIX SORTANT','REVENUE TELCO (Prepaid+Hybrid+OM)','REVENU OM') then valeur_day/1.1925/1.02 else valeur_day end) valeur_day,
    (case when axe_revenu in ('REVENU DATA','REVENU VOIX SORTANT','REVENUE TELCO (Prepaid+Hybrid+OM)','REVENU OM') then valeur_lweek/1.1925/1.02 else valeur_lweek end) valeur_lweek,
    (case when axe_revenu in ('REVENU DATA','REVENU VOIX SORTANT','REVENUE TELCO (Prepaid+Hybrid+OM)','REVENU OM') then v2wa/1.1925/1.02 else v2wa end) v2wa,
    (case when axe_revenu in ('REVENU DATA','REVENU VOIX SORTANT','REVENUE TELCO (Prepaid+Hybrid+OM)','REVENU OM') then v3wa/1.1925/1.02 else v3wa end) v3wa,
    (case when axe_revenu in ('REVENU DATA','REVENU VOIX SORTANT','REVENUE TELCO (Prepaid+Hybrid+OM)','REVENU OM') then v4wa/1.1925/1.02 else v4wa end) v4wa,
    (case when axe_revenu in ('REVENU DATA','REVENU VOIX SORTANT','REVENUE TELCO (Prepaid+Hybrid+OM)','REVENU OM') then valeur_mtd/1.1925/1.02 else valeur_mtd end) valeur_mtd,
    (case when axe_revenu in ('REVENU DATA','REVENU VOIX SORTANT','REVENUE TELCO (Prepaid+Hybrid+OM)','REVENU OM') then valeur_lmtd/1.1925/1.02 else valeur_lmtd end) valeur_lmtd,
    (case when axe_revenu in ('REVENU DATA','REVENU VOIX SORTANT','REVENUE TELCO (Prepaid+Hybrid+OM)','REVENU OM') then budget/1.1925/1.02 else budget end) budget,
    (case when axe_revenu in ('REVENU DATA','REVENU VOIX SORTANT','REVENUE TELCO (Prepaid+Hybrid+OM)','REVENU OM') then budget_lweek/1.1925/1.02 else budget_lweek end) budget_lweek,
    (case when axe_revenu in ('REVENU DATA','REVENU VOIX SORTANT','REVENUE TELCO (Prepaid+Hybrid+OM)','REVENU OM') then budget_2wa/1.1925/1.02 else budget_2wa end) budget_2wa,
    (case when axe_revenu in ('REVENU DATA','REVENU VOIX SORTANT','REVENUE TELCO (Prepaid+Hybrid+OM)','REVENU OM') then budget_3wa/1.1925/1.02 else budget_3wa end) budget_3wa,
    (case when axe_revenu in ('REVENU DATA','REVENU VOIX SORTANT','REVENUE TELCO (Prepaid+Hybrid+OM)','REVENU OM') then budget_4wa/1.1925/1.02 else budget_4wa end) budget_4wa,
    (case when axe_revenu in ('REVENU DATA','REVENU VOIX SORTANT','REVENUE TELCO (Prepaid+Hybrid+OM)','REVENU OM') then valeur_budget_mtd/1.1925/1.02 else valeur_budget_mtd end) valeur_budget_mtd,
    (case when axe_revenu in ('REVENU DATA','REVENU VOIX SORTANT','REVENUE TELCO (Prepaid+Hybrid+OM)','REVENU OM') then valeur_mtd_last_year/1.1925/1.02 else valeur_mtd_last_year end) valeur_mtd_last_year,
    vslweek,
    vs2wa,
    vs3wa,
    vs4wa,
    mtdvslmdt,
    mdtvsbudget,
    weekvsbudget,
    lweekvsblweek,
    v2wavsb2wa,
    v3wavsb3wa,
    v4wavsb4wa,
    mtd_vs_last_year,
    granularite_reg,
    insert_date,
    processing_date
 from mon.SPARK_KPIS_REG_FINAL
)
     where GRANULARITE_REG='NATIONAL'
     group by
     category,
     kpi,
     axe_revenu,
     axe_subscriber,
     axe_vue_transversale,
     processing_date
     order by processing_date desc