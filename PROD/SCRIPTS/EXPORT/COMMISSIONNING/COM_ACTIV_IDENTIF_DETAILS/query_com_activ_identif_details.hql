select 
nvl(identifier_msisdn,'') identifier_msisdn,             
nvl(msisdn_identified,'')  msisdn_identified,           
nvl(est_actives,'') est_actives,          
nvl(est_super_activation,'') est_super_activation,           
nvl(est_bonnes_activation,'') est_bonnes_activation,         
nvl(est_bad_activation,'') est_bad_activation ,          
nvl(est_dry_activation,'') est_dry_activation,            
nvl(recharges,'')  recharges,                 
nvl(recharges_immediates,'') recharges_immediates,           
nvl(recharges_cumulees,'')  recharges_cumulees,         
nvl(delai,'') delai,                   
nvl(est_snappes,'') est_snappes,                 
nvl(est_activation_rech_sup_250,'') est_activation_rech_sup_250 ,  
nvl(est_actives_snappes,'') est_actives_snappes  ,          
nvl(est_actives_snap_rech_sup_250,'') est_actives_snap_rech_sup_250 , 
nvl(to_date(insert_date),'')  insert_date,                 
nvl(motif,'') motif,                   
nvl(to_date(event_date),'') event_date
FROM MON.SPARK_ACTIV_IDENTIF_DETAILS
where EVENT_DATE ='###SLICE_VALUE###'