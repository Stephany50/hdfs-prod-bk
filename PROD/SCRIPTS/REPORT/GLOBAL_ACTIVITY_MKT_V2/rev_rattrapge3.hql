SELECT
  DIM.DT_USAGES.DOMAIN,
  DIM.DT_USAGES.PRODUCT,
  FT_GLOBAL_ACTIVITY_DAILY.SUB_ACCOUNT,
  DIM.DT_DATES.YYYY_MM,
  DIM.DT_DATES.YYYY,
  case when DIM.DT_DATES.YYYYMM=to_char(sysdate,'YYYYMM')  then 1 else 0 end,
  DIM.DT_DATES.dd||' '||DIM.DT_DATES.MON||' '||to_char(DIM.DT_DATES.DATECODE,'yy'),
  DIM.DT_OFFER_PROFILES.CONTRACT_TYPE,
  NVL(DIM.DT_OFFER_PROFILES.HORIZON_OFFER_DESC,FT_GLOBAL_ACTIVITY_DAILY.COMMERCIAL_OFFER_CODE),
  case when DIM.DT_DESTINATIONS.DEST_SHORT like '%CTPhone%' then 'Camtel'
            when DIM.DT_DESTINATIONS.DEST_SHORT like '%Camtel%' then 'Camtel'
            when DIM.DT_DESTINATIONS.DEST_SHORT like '%SVA%' then 'SVA'
            when DIM.DT_DESTINATIONS.DEST_SHORT like '%Roaming%' then 'Roaming'
            when DIM.DT_DESTINATIONS.DEST_SHORT like '%International%' then 'International'
            when DIM.DT_DESTINATIONS.DEST_SHORT like '%Orange%' then 'Orange'
            when DIM.DT_DESTINATIONS.DEST_SHORT like '%MTN%' then 'MTN'
            when DIM.DT_DESTINATIONS.DEST_SHORT like '%NEXTTEL%' then 'NEXTTEL'
            else DIM.DT_DESTINATIONS.DEST_SHORT
        end,
  DIM.DT_USAGES.USAGE_DESCRIPTION,
  DIM.DT_DATES.DATECODE,
  case when DIM.DT_DATES.DATECODE>=trunc(sysdate-45)  then 1 else 0 end,
  case when DIM.DT_DATES.DATECODE>=trunc(sysdate-30)  then 1 else 0 end,
  DIM.DT_DATES.IW,
  case when DIM.DT_DATES.IW=to_char(sysdate,'iw')  then 1 else 0 end,
  case when DIM.DT_DATES.IW>=to_char(sysdate,'iw')-5  then 1 else 0 end,
  case when DIM.DT_DATES.IW=52  then 1 else 0 end,
  FT_GLOBAL_ACTIVITY_DAILY.OPERATOR_CODE,
  DIM.DT_OFFER_PROFILES.SEGMENTATION,
  sum(NVL(FT_GLOBAL_ACTIVITY_DAILY.TAXED_AMOUNT,0))
FROM
  DIM.DT_USAGES,
  FT_GLOBAL_ACTIVITY_DAILY,
  DIM.DT_DATES,
  DIM.DT_OFFER_PROFILES,
  DIM.DT_DESTINATIONS
WHERE
  ( FT_GLOBAL_ACTIVITY_DAILY.TRANSACTION_DATE=DIM.DT_DATES.DATECODE  )
  AND  ( DIM.DT_OFFER_PROFILES.PROFILE_CODE(+)=upper(FT_GLOBAL_ACTIVITY_DAILY.COMMERCIAL_OFFER_CODE)  )
  AND  ( DIM.DT_DESTINATIONS.DEST_ID(+)=FT_GLOBAL_ACTIVITY_DAILY.DESTINATION  )
  AND  ( FT_GLOBAL_ACTIVITY_DAILY.SERVICE_CODE=DIM.DT_USAGES.USAGE_CODE(+)  )
  AND
  (
   ( FT_GLOBAL_ACTIVITY_DAILY.TRAFFIC_MEAN='REVENUE'  )
   AND
   DIM.DT_OFFER_PROFILES.SEGMENTATION  In  @prompt('Saisir une ou plusieurs valeurs pour Segment:','A','Axes d''Analyse\Segment',Multi,Free,Persistent,,User:0)
   AND
   FT_GLOBAL_ACTIVITY_DAILY.OPERATOR_CODE  In  @prompt('Saisir une ou plusieurs valeurs pour Operator Code :','A','Axes d''Analyse\Operator Code',Multi,Free,Persistent,{'OCM'},User:0)
   AND
   FT_GLOBAL_ACTIVITY_DAILY.SUB_ACCOUNT  In  @prompt('Saisir une ou plusieurs valeurs pour Sub Account :','A','Axes d''Analyse\Sub Account',Multi,Free,Persistent,{'MAIN'},User:1)
   AND
   DIM.DT_DATES.DATECODE  BETWEEN  @prompt('EntréeDatecode (Début) :','D','Axes d''Analyse\Datecode',Mono,Free,Persistent,,User:2)  AND  @prompt('EntréeDatecode (Fin) :','D','Axes d''Analyse\Datecode',Mono,Free,Persistent,,User:3)
  )
GROUP BY
  DIM.DT_USAGES.DOMAIN,
  DIM.DT_USAGES.PRODUCT,
  FT_GLOBAL_ACTIVITY_DAILY.SUB_ACCOUNT,
  DIM.DT_DATES.YYYY_MM,
  DIM.DT_DATES.YYYY,
  case when DIM.DT_DATES.YYYYMM=to_char(sysdate,'YYYYMM')  then 1 else 0 end,
  DIM.DT_DATES.dd||' '||DIM.DT_DATES.MON||' '||to_char(DIM.DT_DATES.DATECODE,'yy'),
  DIM.DT_OFFER_PROFILES.CONTRACT_TYPE,
  NVL(DIM.DT_OFFER_PROFILES.HORIZON_OFFER_DESC,FT_GLOBAL_ACTIVITY_DAILY.COMMERCIAL_OFFER_CODE),
  case when DIM.DT_DESTINATIONS.DEST_SHORT like '%CTPhone%' then 'Camtel'
            when DIM.DT_DESTINATIONS.DEST_SHORT like '%Camtel%' then 'Camtel'
            when DIM.DT_DESTINATIONS.DEST_SHORT like '%SVA%' then 'SVA'
            when DIM.DT_DESTINATIONS.DEST_SHORT like '%Roaming%' then 'Roaming'
            when DIM.DT_DESTINATIONS.DEST_SHORT like '%International%' then 'International'
            when DIM.DT_DESTINATIONS.DEST_SHORT like '%Orange%' then 'Orange'
            when DIM.DT_DESTINATIONS.DEST_SHORT like '%MTN%' then 'MTN'
            when DIM.DT_DESTINATIONS.DEST_SHORT like '%NEXTTEL%' then 'NEXTTEL'
            else DIM.DT_DESTINATIONS.DEST_SHORT
        end,
  DIM.DT_USAGES.USAGE_DESCRIPTION,
  DIM.DT_DATES.DATECODE,
  case when DIM.DT_DATES.DATECODE>=trunc(sysdate-45)  then 1 else 0 end,
  case when DIM.DT_DATES.DATECODE>=trunc(sysdate-30)  then 1 else 0 end,
  DIM.DT_DATES.IW,
  case when DIM.DT_DATES.IW=to_char(sysdate,'iw')  then 1 else 0 end,
  case when DIM.DT_DATES.IW>=to_char(sysdate,'iw')-5  then 1 else 0 end,
  case when DIM.DT_DATES.IW=52  then 1 else 0 end,
  FT_GLOBAL_ACTIVITY_DAILY.OPERATOR_CODE,
  DIM.DT_OFFER_PROFILES.SEGMENTATION