SELECT IF(A.FT_DATAMART_OM_MONTH_hold = 0 and E.IT_OMNY_TRANSACTIONS_hold > datediff(last_day(CONCAT('2019-12','-01')), CONCAT('2019-12','-01')),B.FT_OMNY_ACCOUNT_SNAPSHOT_hold_1>0,D.FT_OMNY_ACCOUNT_SNAPSHOT_hold_2>0  ,"OK","NOK")
 FROM
 (SELECT COUNT(*) FT_DATAMART_OM_MONTH_hold FROM MON.FT_DATAMART_OM_MONTH WHERE MOIS= '2019-12') A,
 (SELECT COUNT(*) FT_OMNY_ACCOUNT_SNAPSHOT_hold_1 FROM  MON.FT_OMNY_ACCOUNT_SNAPSHOT WHERE EVENT_DATE = last_day(CONCAT('2019-12','-01')) )B,
 (SELECT COUNT(*) FT_OMNY_ACCOUNT_SNAPSHOT_hold_2 FROM  MON.FT_OMNY_ACCOUNT_SNAPSHOT WHERE EVENT_DATE = CONCAT('2019-12','-01') ) D,
 (SELECT COUNT(DISTINCT TRANSFER_DATETIME) IT_OMNY_TRANSACTIONS_hold FROM CDR.IT_OMNY_TRANSACTIONS WHERE TRANSFER_DATETIME between CONCAT('2019-12','-01') and last_day(concat('2019-12','-01')))  E