 SELECT sum(rated_amount) valeur
         FROM AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_DG a
         where transaction_date ='2021-02-03'   and KPI='PARC' and DESTINATION_CODE = 'USER_30DAYS_GROUP'


SELECT sum(rated_amount) valeur
 FROM AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT a
 where transaction_date ='2021-02-03'  and DESTINATION_CODE = 'USER_30DAYS_GROUP'