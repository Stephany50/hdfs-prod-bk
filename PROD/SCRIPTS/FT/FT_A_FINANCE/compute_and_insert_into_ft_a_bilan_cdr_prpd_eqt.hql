insert into AGG.SPARK_FT_A_BILAN_CDR_PRPD_EQT
SELECT 
    t2.osp_contract_type as profil, 
    t2.commercial_offer as offre,  
    TYPE,  
    BALANCE_IMPACTE, 
    KPI, 
    KPI_DETAILS, 
    DEBIT_CREDIT, 
    COMPTER_CA_RECHARGE, 
    COMPTER_EQT_PREP, 
    sum(t1.main_credit) as main_credit,  
    sum(main_debit) as main_debit, 
    sum(loan_credit) as loan_credit, 
    sum(loan_debit) as loan_debit, 
    sum(total_credit) as total_credit,
    sum(total_debit) as total_debit, 
    sum(montant_principal) as montant_principal, 
    sum(montant_secondaire) as montant_secondaire, 
    sum(nombre) as nombre ,
    jour
    FROM(SELECT  EVENT_DATE as jour, TYPE, ACCT_ID_MSISDN as MSISDN, t2.BALANCE_IMPACTE, t2.KPI, t2.KPI_DETAILS, t2.DEBIT_CREDIT, t2.COMPTER_CA_RECHARGE, t2.COMPTER_EQT_PREP,sum(main_credit) as main_credit, sum(main_debit) as main_debit,sum(loan_credit) as loan_credit, sum(loan_debit) as loan_debit, sum(main_credit+loan_credit) as total_credit,sum(main_debit+loan_debit) as total_debit, sum(case when (main_credit+loan_credit) < (main_debit+loan_debit) then main_debit+loan_debit else main_credit+loan_credit end) as montant_principal, sum(case when (main_credit+loan_credit) >= (main_debit+loan_debit) then main_debit+loan_debit else main_credit+loan_credit end) as montant_secondaire, count(event_time) as nombre FROM MON.SPARK_FT_EDR_PRPD_EQT t1 
    LEFT OUTER JOIN RA.EQTPREP_DESCRIPTION_FLUX t2 
    ON t1.TYPE=t2.type_flux WHERE EVENT_DATE = '###SLICE_VALUE###' 
    group by  EVENT_DATE, TYPE, ACCT_ID_MSISDN, t2.BALANCE_IMPACTE, t2.KPI, t2.KPI_DETAILS, t2.DEBIT_CREDIT, t2.COMPTER_CA_RECHARGE, t2.COMPTER_EQT_PREP)  t1
left outer join (SELECT event_date, access_key,  osp_contract_type, commercial_offer FROM mon.spark_ft_contract_snapshot WHERE event_date = '###SLICE_VALUE###') t2  
on t1.MSISDN = t2.access_key and  t1.jour = t2.event_date 
group by  jour, TYPE, t2.osp_contract_type, t2.commercial_offer, BALANCE_IMPACTE, KPI,KPI_DETAILS, DEBIT_CREDIT, COMPTER_CA_RECHARGE, COMPTER_EQT_PREP