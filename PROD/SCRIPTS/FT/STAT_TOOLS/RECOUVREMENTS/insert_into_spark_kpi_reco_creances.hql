-- chiffre d'affaire par cycle de facturation
INSERT INTO AGG.SPARK_KPI_RECO_CREANCES
SELECT
'CA' KPI,
SUM(INVOICE_AMOUNT) VAL,
CURRENT_TIMESTAMP() INSERT_DATE,
'###SLICE_VALUE###' EVENT_MONTH
FROM CDR.SPARK_IT_CHIFFRE_AFFAIRE
WHERE ORDER_DATE like "###SLICE_VALUE###%"
UNION ALL
-- valeur encaissée
SELECT
'VALEUR_ENCAISS' KPI,
SUM(MONTANT) VAL,
CURRENT_TIMESTAMP() INSERT_DATE,
'###SLICE_VALUE###' EVENT_MONTH
FROM CDR.SPARK_IT_RAPPORT_DAILY
WHERE DATE_SAISIE between CONCAT('###SLICE_VALUE###','-','01') and date_add(add_months(CONCAT('###SLICE_VALUE###','-','01'),1),-1)
UNION ALL
-- objectif encaissement
SELECT
'OBJECT_ENC' KPI,
(SUM(INVOICE_AMOUNT * 0.95) + 60000000) VAL,
CURRENT_TIMESTAMP() INSERT_DATE,
'###SLICE_VALUE###' EVENT_MONTH
FROM CDR.SPARK_IT_CHIFFRE_AFFAIRE
WHERE ORDER_DATE between add_months(CONCAT('###SLICE_VALUE###','-','01'),-1) and date_add(CONCAT('###SLICE_VALUE###','-','01'),-1)
UNION ALL
-- taux recouvrement global = VALEUR_ENCAISS / OBJECT_ENC
-- taux recouvrement 30j
SELECT
'TAUX_30J' KPI,
SUM(MONTANT) VAL,
CURRENT_TIMESTAMP() INSERT_DATE,
'###SLICE_VALUE###' EVENT_MONTH
FROM CDR.SPARK_IT_RAPPORT_DAILY
WHERE DATE_SAISIE between CONCAT('###SLICE_VALUE###','-','01') and date_add(add_months(CONCAT('###SLICE_VALUE###','-','01'),1),-1)
AND INVOICE_DATE between add_months(CONCAT('###SLICE_VALUE###','-','01'),-1) and date_add(CONCAT('###SLICE_VALUE###','-','01'),-1)
UNION ALL
-- taux recouvrement 60j
SELECT
'TAUX_60J' KPI,
SUM(MONTANT) VAL,
CURRENT_TIMESTAMP() INSERT_DATE,
'###SLICE_VALUE###' EVENT_MONTH
FROM CDR.SPARK_IT_RAPPORT_DAILY
WHERE DATE_SAISIE between CONCAT('###SLICE_VALUE###','-','01') and date_add(add_months(CONCAT('###SLICE_VALUE###','-','01'),1),-1)
AND INVOICE_DATE between add_months(CONCAT('###SLICE_VALUE###','-','01'),-2) and date_add(add_months(CONCAT('###SLICE_VALUE###','-','01'),-1),-1)
UNION ALL
-- taux recouvrement 90j
SELECT
'TAUX_90J' KPI,
SUM(MONTANT) VAL,
CURRENT_TIMESTAMP() INSERT_DATE,
'###SLICE_VALUE###' EVENT_MONTH
FROM CDR.SPARK_IT_RAPPORT_DAILY
WHERE DATE_SAISIE between CONCAT('###SLICE_VALUE###','-','01') and date_add(add_months(CONCAT('###SLICE_VALUE###','-','01'),1),-1)
AND INVOICE_DATE between add_months(CONCAT('###SLICE_VALUE###','-','01'),-3) and date_add(add_months(CONCAT('###SLICE_VALUE###','-','01'),-2),-1)
-- taux recouvrement >90j
UNION ALL
SELECT
'TAUX_90J_PLUS' KPI,
SUM(MONTANT) VAL,
CURRENT_TIMESTAMP() INSERT_DATE,
'###SLICE_VALUE###' EVENT_MONTH
FROM CDR.SPARK_IT_RAPPORT_DAILY
WHERE DATE_SAISIE between CONCAT('###SLICE_VALUE###','-','01') and date_add(add_months(CONCAT('###SLICE_VALUE###','-','01'),1),-1)
AND INVOICE_DATE < add_months(CONCAT('###SLICE_VALUE###','-','01'),-3)
UNION ALL
-- Nombre de clients suspendus global
SELECT
'NB_CLIENT_SUSP' KPI,
COUNT(distinct Account_number) VAL,
CURRENT_TIMESTAMP() INSERT_DATE,
'###SLICE_VALUE###' EVENT_MONTH
FROM CDR.SPARK_IT_SUSPENSION_DAILY
WHERE DATE_SUSPENSION between CONCAT('###SLICE_VALUE###','-','01') and date_add(add_months(CONCAT('###SLICE_VALUE###','-','01'),1),-1)
UNION ALL
-- Solde créances du mois en cours
SELECT
'SOLDE_CR_M' KPI,
SUM(Balance) VAL,
CURRENT_TIMESTAMP() INSERT_DATE,
'###SLICE_VALUE###' EVENT_MONTH
FROM CDR.SPARK_IT_BALANCE_AGEE
WHERE AS_OF_DATE between CONCAT('###SLICE_VALUE###','-','01') and LAST_DAY(CONCAT('###SLICE_VALUE###','-01'))
UNION ALL
-- Solde créances du mois précedent
SELECT
'SOLDE_CR_M_1' KPI,
SUM(Balance) VAL,
CURRENT_TIMESTAMP() INSERT_DATE,
'###SLICE_VALUE###' EVENT_MONTH
FROM CDR.SPARK_IT_BALANCE_AGEE
WHERE AS_OF_DATE between add_months(CONCAT('###SLICE_VALUE###','-','01'),-1) and date_add(CONCAT('###SLICE_VALUE###','-','01'),-1)
UNION ALL
---- éléments du risque de dotation mensuel
-- premier element
SELECT
'CR_RDM_DEB_90j_PLUS' KPI,
SUM(nvl(120jrs, 0) + nvl(150jrs, 0) + nvl(180jrs, 0) + nvl(360jrs, 0) + nvl(720jrs, 0) + nvl(1080jrs, 0) + nvl(1440jrs, 0) + nvl(1800jrs, 0) + nvl(plus_1800jrs, 0)) VAL,
CURRENT_TIMESTAMP() INSERT_DATE,
'###SLICE_VALUE###' EVENT_MONTH
FROM CDR.SPARK_IT_BALANCE_AGEE BA
LEFT JOIN (select * from DIM.RC_RETRAITEMENT_ETAT_VIP where type='ETAT') EV ON (trim(BA.account_number) = trim(EV.account_nb))
LEFT JOIN DIM.RC_RETRAITEMENT_REVENDEURS R ON (trim(BA.account_number) = trim(R.account_nb))
LEFT JOIN DIM.RC_RETRAITEMENT_EMPLOYEE_TEST ET ON (trim(BA.account_number) = trim(ET.account_nb))
WHERE AS_OF_DATE = '###SLICE_VALUE###-05'
and BA.STATUT in ('Active', 'Suspended')
and BA.balance > 0
and EV.account_nb is null
and R.account_nb is null
and ET.account_nb is null
-- deuxième element
UNION ALL
SELECT
'CR_RDM_CLIENTS_DESAC' KPI,
SUM(Balance) VAL,
CURRENT_TIMESTAMP() INSERT_DATE,
'###SLICE_VALUE###' EVENT_MONTH
FROM CDR.SPARK_IT_BALANCE_AGEE BA
LEFT JOIN (select * from DIM.RC_RETRAITEMENT_ETAT_VIP where type='ETAT') EV ON (trim(BA.account_number) = trim(EV.account_nb))
LEFT JOIN DIM.RC_RETRAITEMENT_REVENDEURS R ON (trim(BA.account_number) = trim(R.account_nb))
LEFT JOIN DIM.RC_RETRAITEMENT_EMPLOYEE_TEST ET ON (trim(BA.account_number) = trim(ET.account_nb))
WHERE AS_OF_DATE = '###SLICE_VALUE###-05'
and BA.STATUT in ('Inactive')
and BA.balance > 0
and EV.account_nb is null
and R.account_nb is null
and ET.account_nb is null
UNION ALL
SELECT
---- éléments atterissage mensuel
-- premier element
'CR_AM_DEB_90j_PLUS' KPI,
SUM(nvl(120jrs, 0) + nvl(150jrs, 0) + nvl(180jrs, 0) + nvl(360jrs, 0) + nvl(720jrs, 0) + nvl(1080jrs, 0) + nvl(1440jrs, 0) + nvl(1800jrs, 0) + nvl(plus_1800jrs, 0)) VAL,
CURRENT_TIMESTAMP() INSERT_DATE,
'###SLICE_VALUE###' EVENT_MONTH
FROM CDR.SPARK_IT_BALANCE_AGEE BA
LEFT JOIN (select * from DIM.RC_RETRAITEMENT_ETAT_VIP where type='ETAT') EV ON (trim(BA.account_number) = trim(EV.account_nb))
LEFT JOIN DIM.RC_RETRAITEMENT_REVENDEURS R ON (trim(BA.account_number) = trim(R.account_nb))
LEFT JOIN DIM.RC_RETRAITEMENT_EMPLOYEE_TEST ET ON (trim(BA.account_number) = trim(ET.account_nb))
WHERE AS_OF_DATE = date_add(add_months(CONCAT('###SLICE_VALUE###','-','01'),1),-1)
and BA.STATUT in ('Active', 'Suspended')
and BA.balance > 0
and EV.account_nb is null
and R.account_nb is null
and ET.account_nb is null
UNION ALL
-- deuxième element
SELECT
'CR_AM_CLIENTS_DESAC' KPI,
SUM(Balance) VAL,
CURRENT_TIMESTAMP() INSERT_DATE,
'###SLICE_VALUE###' EVENT_MONTH
FROM CDR.SPARK_IT_BALANCE_AGEE BA
LEFT JOIN (select * from DIM.RC_RETRAITEMENT_ETAT_VIP where type='ETAT') EV ON (trim(BA.account_number) = trim(EV.account_nb))
LEFT JOIN DIM.RC_RETRAITEMENT_REVENDEURS R ON (trim(BA.account_number) = trim(R.account_nb))
LEFT JOIN DIM.RC_RETRAITEMENT_EMPLOYEE_TEST ET ON (trim(BA.account_number) = trim(ET.account_nb))
WHERE AS_OF_DATE = date_add(add_months(CONCAT('###SLICE_VALUE###','-','01'),1),-1)
and BA.STATUT in ('Inactive')
and BA.balance > 0
and EV.account_nb is null
and R.account_nb is null
and ET.account_nb is null
