insert into MON.SPARK_FT_A_CXD_RECOUVREMENT
select
  a.categ,
  a.statut,
  sum(
    case 
      when a.statut in ("Active") then 1 
      else 0 
    end
  ) nbr_client_actif,
  sum(
    case 
      when a.statut in ("Suspended") then 1 
      else 0 
    end
  ) nbr_client_suspendu,
  sum(
    case 
      when a.statut in ("Inactive") then 1 
      else 0 
    end
  ) nbr_client_inactif,
  sum(
    case 
      when balance>0 then balance 
      else 0 
    end
  ) creance,
  sum(
    case 
      when 0jrs>0 then 0jrs 
      else 0 
    end
  ) 0jrs,
  sum(
    case 
      when 30jrs>0 then 30jrs 
      else 0 
    end
  ) 30jrs,
  sum(
    case 
      when 60jrs>0 then 60jrs 
      else 0 
    end
  ) 60jrs,
  sum(
    case 
      when 90jrs>0 then 90jrs 
      else 0 
    end
  ) 90jrs,
  sum(
      if(120jrs>0,120jrs,0)+if(150jrs>0,150jrs,0)+
    if(180jrs>0,180jrs,0)+if(360jrs>0,360jrs,0)+
    if(720jrs>0,720jrs,0)+if(1080jrs>0,1080jrs,0)+
    if(1440jrs>0,1440jrs,0)+if(1800jrs>0,1800jrs,0)+
    if(plus_1800jrs>0,plus_1800jrs,0)
  ) Plus_120jrs,
  sum(
    case 
      when a.categ not in ("Partner","Key Accounts","O-Shop", "Employé","Corporate") and b.account_number is null then  (case when a.statut = "Inactive" then if(balance>0,balance,0)
      else if(120jrs>0,120jrs,0)+if(150jrs>0,150jrs,0)+
      if(180jrs>0,180jrs,0)+if(360jrs>0,360jrs,0)+
      if(720jrs>0,720jrs,0)+if(1080jrs>0,1080jrs,0)+
      if(1440jrs>0,1440jrs,0)+if(1800jrs>0,1800jrs,0)+
      if(plus_1800jrs>0,plus_1800jrs,0) end)
      else 0 end
  ) badDebt,
  (
    max(c.badDebt) - sum(case when a.categ not in ("Partner","Key Accounts","O-Shop", "Employé","Corporate") and b.account_number is null then 
    (case when a.statut = "Inactive" then if(balance>0,balance,0)
      else if(120jrs>0,120jrs,0)+if(150jrs>0,150jrs,0)+
      if(180jrs>0,180jrs,0)+if(360jrs>0,360jrs,0)+
      if(720jrs>0,720jrs,0)+if(1080jrs>0,1080jrs,0)+
      if(1440jrs>0,1440jrs,0)+if(1800jrs>0,1800jrs,0)+
      if(plus_1800jrs>0,plus_1800jrs,0) end)
    else 0 end)
  ) risqueDot,
  current_timestamp insert_date,
  to_date(a.as_of_date) event_date
from CDR.SPARK_IT_BALANCE_AGEE a
left join 
(
  select distinct translate(account_number,",",".") account_number 
  from TMP.LISTE_EXCLUSION_B2B
) b 
on translate(a.account_number,",",".")=b.account_number 
left join 
(
  select categ,statut,badDebt 
  from MON.SPARK_FT_A_CXD_RECOUVREMENT 
  where event_date=last_day(add_months("###SLICE_VALUE###",-1))
) c on c.categ=a.categ and c.statut=a.statut
where as_of_date="###SLICE_VALUE###" 
group by as_of_date,a.categ,a.statut