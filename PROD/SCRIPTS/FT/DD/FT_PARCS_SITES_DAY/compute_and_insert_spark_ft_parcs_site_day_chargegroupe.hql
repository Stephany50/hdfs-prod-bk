INSERT INTO  MON.SPARK_FT_PARCS_SITE_DAY
select 
    'PARC_CHARGE_GRP' parc_type
    ,'profile' profile
    ,'ACTIF'  STATUT
    ,sum(case when parc_Charge_grp = 'ACTIF' then 1 else 0 end) effectif
    ,b.SITE_NAME
    ,b.townname
    ,b.administrative_region
    ,b.commercial_region
    ,'src_table' src_table
    ,CURRENT_TIMESTAMP  INSERT_DATE
    ,'CONTRACT_TYPE' CONTRACT_TYPE
    ,'OCM' operator_code,b.event_date
        from         
            (
            select distinct msisdn msisdn
                ,event_date
                ,(CASE
                        WHEN  (( TO_DATE(b.OG_CALL)  > DATE_ADD('###SLICE_VALUE###',-30)  ) 
                        OR ( TO_DATE(b.IC_CALL_4) > DATE_ADD('###SLICE_VALUE###',-30) and TO_DATE(b.IC_CALL_3) > DATE_ADD('###SLICE_VALUE###',-30) and TO_DATE(b.IC_CALL_2) > DATE_ADD('###SLICE_VALUE###',-30) and TO_DATE(b.IC_CALL_1) > DATE_ADD('###SLICE_VALUE###',-30))
                            ) THEN 'ACTIF'
                        ELSE 'INACT'
                        END         ) Parc_Charge_grp
                        
                from MON.SPARK_FT_ACCOUNT_ACTIVITY b
                where event_date ='###SLICE_VALUE###'
                ) a
         left join  
          (
          select Distinct MSISDN,EVENT_DATE,SITE_NAME,LAST_LOCATION_DAY,townname,administrative_region,commercial_region 
            from 
              MON.SPARK_FT_CLIENT_LAST_SITE_DAY 
              where EVENT_DATE =DATE_ADD('###SLICE_VALUE###',-1)
            ) b on a.msisdn=b.msisdn
  group by 
  b.SITE_NAME
  ,b.townname
  ,b.administrative_region
  ,b.commercial_region
  ,b.event_date
