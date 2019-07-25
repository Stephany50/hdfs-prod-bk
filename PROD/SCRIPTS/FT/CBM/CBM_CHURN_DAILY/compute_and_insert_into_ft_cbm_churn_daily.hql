INSERT INTO MON.FT_CBM_CHURN_DAILY
SELECT
       A.msisdn,
       A.lost_type,
       B.site_name SITE,
       B.townname VILLE,
       B.administrative_region REGION,
       B.last_location_day DATE_DERNIERE_LOCALISATION,
       A.activation_date,
       current_timestamp INSERT_DATE,
       A.event_date
FROM   (
             SELECT
               msisdn,
               activation_date,
               '###slice_value###' EVENT_DATE,
               ( CASE
                   WHEN ( og_call = date_sub('###slice_value###' , 31) AND Least(ic_call_4, ic_call_3, ic_call_2, ic_call_1) <=date_sub('###slice_value###',31))
                   OR( Least(ic_call_4, ic_call_3, ic_call_2, ic_call_1)=date_sub('###slice_value###',31)AND og_call <= date_sub('###slice_value###',31 ))
                   THEN '30DAYSLOST'
                   WHEN ( og_call =date_sub('###slice_value###',41) AND Least(ic_call_4, ic_call_3, ic_call_2, ic_call_1) <=date_sub('###slice_value###',41))
                         OR ( Least(ic_call_4, ic_call_3, ic_call_2, ic_call_1)=date_sub('###slice_value###',41) AND og_call <= date_sub('###slice_value###',41))
                   THEN '40DAYSLOST'
                   WHEN ( og_call =date_sub('###slice_value###',51) AND Least(ic_call_4, ic_call_3, ic_call_2, ic_call_1) <=date_sub('###slice_value###',51))
                         OR ( Least(ic_call_4, ic_call_3, ic_call_2, ic_call_1)=date_sub('###slice_value###',51) AND og_call <= date_sub('###slice_value###',51 ))
                   THEN '50DAYSLOST'
                   WHEN ( og_call =date_sub('###slice_value###',61) AND Least(ic_call_4, ic_call_3, ic_call_2, ic_call_1) <=date_sub('###slice_value###',61 ))
                         OR ( Least(ic_call_4, ic_call_3, ic_call_2, ic_call_1)=date_sub('###slice_value###',61) AND og_call <= date_sub('###slice_value###',61))
                   THEN '60DAYSLOST'
                   WHEN ( og_call =date_sub('###slice_value###',71) AND Least(ic_call_4, ic_call_3, ic_call_2, ic_call_1) <=date_sub('###slice_value###',71))
                         OR ( Least(ic_call_4, ic_call_3, ic_call_2, ic_call_1)=date_sub('###slice_value###',71) AND og_call <= date_sub('###slice_value###',71))
                   THEN '70DAYSLOST'
                   WHEN ( og_call =date_sub('###slice_value###',81) AND Least(ic_call_4, ic_call_3, ic_call_2, ic_call_1) <=date_sub('###slice_value###',81))
                         OR ( Least(ic_call_4, ic_call_3, ic_call_2, ic_call_1)=date_sub('###slice_value###',81) AND og_call <=date_sub('###slice_value###',81))
                   THEN '80DAYSLOST'
                   WHEN ( og_call =date_sub('###slice_value###',91) AND Least(ic_call_4, ic_call_3, ic_call_2, ic_call_1) <=date_sub('###slice_value###',91))
                         OR ( Least(ic_call_4, ic_call_3, ic_call_2, ic_call_1)=date_sub('###slice_value###',91) AND og_call <=date_sub('###slice_value###',91))
                   THEN '90DAYSLOST'
                   ELSE NULL
                 END ) LOST_TYPE
        FROM   (SELECT * FROM   AGG.FT_ACCOUNT_ACTIVITY WHERE event_date = date_add( '###slice_value###', 1)) A
        WHERE
                (og_call =date_sub('###slice_value###',31) AND Least(ic_call_4, ic_call_3, ic_call_2, ic_call_1) <=date_sub('###slice_value###',31))
                OR ( Least(ic_call_4, ic_call_3, ic_call_2, ic_call_1) =date_sub('###slice_value###',31) AND og_call <=date_sub('###slice_value###',31))
                OR ( og_call =date_sub('###slice_value###',41) AND Least(ic_call_4, ic_call_3, ic_call_2, ic_call_1) <=date_sub('###slice_value###',41))
                OR ( Least(ic_call_4, ic_call_3, ic_call_2, ic_call_1) =date_sub('###slice_value###',41) AND og_call <=date_sub('###slice_value###',41))
                OR ( og_call =date_sub('###slice_value###',51) AND Least(ic_call_4, ic_call_3, ic_call_2, ic_call_1) <=date_sub('###slice_value###',51))
                OR ( Least(ic_call_4, ic_call_3, ic_call_2, ic_call_1) =date_sub('###slice_value###',51) AND og_call <=date_sub('###slice_value###',51))
                OR ( og_call =date_sub('###slice_value###',61) AND Least(ic_call_4, ic_call_3, ic_call_2, ic_call_1) <=date_sub('###slice_value###',61))
                OR ( Least(ic_call_4, ic_call_3, ic_call_2, ic_call_1) =date_sub('###slice_value###',61) AND og_call <=date_sub('###slice_value###',61))
                OR ( og_call =date_sub('###slice_value###',71) AND Least(ic_call_4, ic_call_3, ic_call_2, ic_call_1) <=date_sub('###slice_value###',71))
                OR ( Least(ic_call_4, ic_call_3, ic_call_2, ic_call_1) =date_sub('###slice_value###',71) AND og_call <=date_sub('###slice_value###',71))
                OR ( og_call =date_sub('###slice_value###',81) AND Least(ic_call_4, ic_call_3, ic_call_2, ic_call_1) <=date_sub('###slice_value###',81))
                OR ( Least(ic_call_4, ic_call_3, ic_call_2, ic_call_1) =date_sub('###slice_value###',81) AND og_call <=date_sub('###slice_value###',81))
                OR ( og_call =date_sub('###slice_value###',91) AND Least(ic_call_4, ic_call_3, ic_call_2, ic_call_1) <=date_sub('###slice_value###',91))
                OR ( Least(ic_call_4, ic_call_3, ic_call_2, ic_call_1) =date_sub('###slice_value###',91) AND og_call <=date_sub('###slice_value###',91))
        )A
       LEFT JOIN (SELECT A.*
                  FROM   mon.ft_client_last_site_day A
                  WHERE  event_date = '###slice_value###') B
              ON A.msisdn = B.msisdn;
