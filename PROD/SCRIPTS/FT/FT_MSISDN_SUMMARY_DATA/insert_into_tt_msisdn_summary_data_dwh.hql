

--Insertion des données liées à la couverture la technologie et le status de l'abonné
insert into TT_MSISDN_SUMMARY_DATA_TRAFIC_DATA_SITE (event_date, msisdn, couverture, technologie, status, bytes_used, last_imei_used)
select event_date, msisdn , couverture, technologie
, case when bytes_used > 0 then 'User_Data'
    else 'Not_User_Data'
    end status
, bytes_used, imei
from
(
select *
from FT_MSISDN_TRAFIC_DATA_SITE
where event_date = d_slice_value        -- '02/01/2017'        --d_slice_value--between  '01/01/2017' and '31/01/2017'
)a
,(
select site_name, max(technologie) couverture
from MON.VW_SDT_CI_INFO --dim.dt_gsm_cell_code
group by site_name
)b
,  (select tac_code, max(technologie) technologie from dim.dt_handset_ref group by tac_code) 
where a.site_name = b.site_name(+)
and substr(imei, 1, 8) = tac_code(+);
    
commit;  

--Vérifier qu'on est pas en début de mois:
--IF TO_CHAR(d_slice_value, 'dd') <> '01' THEN




merge into TT_MSISDN_SUMMARY_DATA a
using
(
    select * from FT_MSISDN_SUMMARY_DATA 
    where event_date =prev_slice_value
) b
on 
( a.msisdn = b.msisdn )
when matched then
    update set 
    a.Last_couv_4G = case when a.couverture = '4G' then a.event_date else b.Last_couv_4G end
    , a.First_couv_4G = case when a.couverture = '4G' then nvl(b.First_couv_4G, a.event_date) else b.First_couv_4G end
    , a.Count_couv_4G = case when a.couverture = '4G' then nvl(b.Count_couv_4G, 0) +1 else b.Count_couv_4G end  
    , a.Last_couv_3G = case when a.couverture = '3G' then a.event_date else b.Last_couv_3G end
    , a.First_couv_3G = case when a.couverture = '3G' then nvl(b.First_couv_3G, a.event_date) else b.First_couv_3G end
    , a.Count_couv_3G = case when a.couverture = '3G' then nvl(b.Count_couv_3G, 0) +1 else b.Count_couv_3G end 
    , a.Last_couv_2G = case when a.couverture = '2G' then a.event_date else b.Last_couv_2G  end  
    , a.First_couv_2G = case when a.couverture = '2G' then nvl(b.First_couv_2G, a.event_date) else b.First_couv_2G end
    , a.Count_couv_2G = case when a.couverture = '2G' then nvl(b.Count_couv_2G, 0) +1 else b.Count_couv_2G end 
    , a.First_Phone_4G = case when a.Technologie = '4G' then nvl(b.First_Phone_4G,  a.event_date) else b.First_Phone_4G end 
    , a.Last_Phone_4G = case when a.Technologie = '4G' then a.event_date else b.Last_Phone_4G end    
    , a.Count_phone_4G = case when a.Technologie = '4G' then nvl(b.Count_Phone_4G, 0) + 1 else b.Count_Phone_4G end 
    , a.First_Phone_3G = case when a.Technologie in ('3G', '2.75G', '2.5G') then nvl(b.First_Phone_3G,  a.event_date) else b.First_Phone_3G end
    , a.Last_Phone_3G = case when a.Technologie in ('3G', '2.75G', '2.5G') then a.event_date else b.Last_Phone_3G end
    , a.Count_phone_3G = case when a.Technologie in ('3G', '2.75G', '2.5G')  then nvl(b.Count_Phone_3G, 0) + 1 else b.Count_Phone_3G end
    , a.First_Phone_2G = case when a.Technologie = '2G' then nvl(b.First_Phone_2G,  a.event_date) else b.First_Phone_2G end 
    , a.Last_Phone_2G = case when a.Technologie = '2G' then a.event_date else b.Last_Phone_2G end
    , a.Count_phone_2G = case when a.Technologie = '2G' then nvl(b.Count_Phone_2G, 0) + 1 else b.Count_Phone_2G end  
    , a.First_Using_Data = case when a.status = 'User_Data' then nvl(b.First_using_Data, a.event_date) else b.First_Using_Data end  
    , a.Last_Using_Data = case when a.status = 'User_Data' then a.event_date else b.Last_Using_Data end
    , Count_Using_Data = case when a.status = 'User_Data' then nvl(b.count_Using_Data, 0) + 1 else b.Count_Using_Data end 
    , a.MAX_BYTES_USED = case when nvl(a.BYTES_USED, 0) >= nvl(b.MAX_BYTES_USED, 0) then nvl(a.BYTES_USED, 0) else nvl(b.MAX_BYTES_USED, 0) end
    , a.DATE_MAX_BYTES_USED = case when nvl(a.BYTES_USED, 0) >= nvl(b.MAX_BYTES_USED, 0) then a.EVENT_DATE else b.DATE_MAX_BYTES_USED  end 
    , a.last_imei_used = nvl(a.last_imei_used, b.last_imei_used)
    , insert_date = sysdate  
when not matched then
    insert (a.EVENT_DATE, a.MSISDN, a.COUVERTURE, a.TECHNOLOGIE, a.STATUS, a.FIRST_COUV_4G, a.LAST_COUV_4G, a.COUNT_COUV_4G, a.FIRST_COUV_3G, a.LAST_COUV_3G
    , a.COUNT_COUV_3G, a.FIRST_COUV_2G, a.LAST_COUV_2G, a.COUNT_COUV_2G, a.FIRST_PHONE_4G, a.LAST_PHONE_4G, a.COUNT_PHONE_4G, a.FIRST_PHONE_3G, a.LAST_PHONE_3G
    , a.COUNT_PHONE_3G, a.FIRST_PHONE_2G, a.LAST_PHONE_2G, a.COUNT_PHONE_2G, a.FIRST_USING_DATA, a.LAST_USING_DATA, a.COUNT_USING_DATA, a.insert_date, last_imei_used)
    values (d_slice_value, b.MSISDN, b.COUVERTURE, b.TECHNOLOGIE, b.STATUS, b.FIRST_COUV_4G, b.LAST_COUV_4G, b.COUNT_COUV_4G, b.FIRST_COUV_3G, b.LAST_COUV_3G
    , b.COUNT_COUV_3G, b.FIRST_COUV_2G, b.LAST_COUV_2G, b.COUNT_COUV_2G, b.FIRST_PHONE_4G, b.LAST_PHONE_4G, b.COUNT_PHONE_4G, b.FIRST_PHONE_3G, b.LAST_PHONE_3G
    , b.COUNT_PHONE_3G, b.FIRST_PHONE_2G, b.LAST_PHONE_2G, b.COUNT_PHONE_2G, b.FIRST_USING_DATA, b.LAST_USING_DATA, b.COUNT_USING_DATA, sysdate, b.last_imei_used);
          --
         COMMIT; 
         
 
            SELECT  ( CASE
                    -- si aucune ligne
                    WHEN  (SELECT COUNT (*) nbre FROM MON.FT_CONTRACT_SNAPSHOT WHERE EVENT_DATE = d_slice_value  AND  ROWNUM < 2  )  = 0 THEN 0
                    ELSE  1 -- si une ligne existe alors marquer comme vrai -- exception
                    END ) svalue INTO n_is_day_activity_done FROM DUAL
                    ;
            IF n_is_day_activity_done = 1 THEN  
            
                merge into TT_MSISDN_SUMMARY_DATA  S
                using (select event_date, access_key, max(osp_status) osp_status
                        from ft_contract_snapshot
                        where event_date = d_slice_value    --'02/11/2016'
                        group by event_date, access_key
                        ) D
                on ( S.event_date = d_slice_value   --'02/11/2016'
                    and msisdn = access_key
                    )
                when matched then
                update set S.OSP_STATUS = D.OSP_STATUS;
                
                 commit;  
            END IF;    
             
                        --Mise a jour des données relatives à otarie:
            SELECT  ( CASE
            -- si aucune ligne
                    WHEN  (SELECT COUNT (*) nbre FROM FT_OTARIE_DATA_TRAFFIC_DAY WHERE TRANSACTION_DATE = d_slice_value  AND  ROWNUM < 2) =1 
                            --AND (SELECT COUNT (*) nbre FROM FT_MSISDN_SUMMARY_DATA WHERE EVENT_DATE = d_slice_value  AND  ROWNUM < 2 )  = 1 
                 THEN 1
            ELSE  0 -- si une ligne existe alors marquer comme vrai -- exception  
            END ) svalue INTO N_IS_OTARIE_DONE FROM DUAL ; 
            
            
            IF n_is_otarie_done = 1 THEN  
            merge into TT_MSISDN_SUMMARY_DATA  Dest
            using 
            (
            select TRANSACTION_DATE, MSISDN  
                , sum(case when RADIO_ACCESS_TECHNO = '2G' 
                        then Nbytest else 0 
                        end) Otarie_BYTES_2G 
                , sum(case when RADIO_ACCESS_TECHNO in ('3G', 'HSPA') then Nbytest else 0 end) Otarie_Bytes_3G
                , sum(case when RADIO_ACCESS_TECHNO = 'LTE' then Nbytest else 0 end) Otarie_Bytes_4G
                , sum(case when RADIO_ACCESS_TECHNO = 'Unknown' then Nbytest else 0 end) Otarie_Bytes_Ukn 
            from FT_OTARIE_DATA_TRAFFIC_DAY
            where transaction_date = d_slice_value  --'03/11/2016'
            group by  TRANSACTION_DATE, MSISDN
            ) Src
            on ( Dest.msisdn = Src.msisdn
            and event_date = d_slice_value )--'02/11/2016')
            when matched then 
                update set Dest.Otarie_Bytes_2G = Src.Otarie_Bytes_2G
                    , Dest.Otarie_Bytes_3G = Src.Otarie_Bytes_3G
                    , Dest.Otarie_Bytes_4G = Src.Otarie_Bytes_4G
                    , Dest.Otarie_Bytes_Ukn = Src.Otarie_Bytes_Ukn  
             when not matched then 
                        insert (event_date, msisdn, otarie_bytes_2G, otarie_bytes_3G, otarie_bytes_4G, otarie_bytes_Ukn)
                            values(transaction_date, Src.msisdn, Src.otarie_bytes_2G, Src.otarie_bytes_3G, Src.otarie_bytes_4G, Src.otarie_bytes_Ukn);
                    commit;  



    
insert/*append*/ into FT_MSISDN_SUMMARY_DATA (EVENT_DATE, MSISDN, COUVERTURE, TECHNOLOGIE, STATUS, FIRST_COUV_4G, LAST_COUV_4G, COUNT_COUV_4G
, FIRST_COUV_3G, LAST_COUV_3G, COUNT_COUV_3G, FIRST_COUV_2G, LAST_COUV_2G, COUNT_COUV_2G, FIRST_PHONE_4G, LAST_PHONE_4G, COUNT_PHONE_4G
, FIRST_PHONE_3G, LAST_PHONE_3G, COUNT_PHONE_3G, FIRST_PHONE_2G, LAST_PHONE_2G, COUNT_PHONE_2G, FIRST_USING_DATA, LAST_USING_DATA, COUNT_USING_DATA
, INSERT_DATE, BYTES_USED, MAX_BYTES_USED, DATE_MAX_BYTES_USED, OSP_STATUS, Otarie_First_RAT, otarie_bytes_2G, otarie_bytes_3G, otarie_bytes_4G, otarie_bytes_Ukn
, last_imei_used) 
select * from TT_MSISDN_SUMMARY_DATA;

insert into LOG_ACTIVATION_DAILY values ('MSISDN_SUMMARY_DATA', d_slice_value, sysdate, sysdate, 1);

commit;

s_sql_query := 'Truncate table TT_MSISDN_SUMMARY_DATA';


execute immediate s_sql_query;

         -- fin
         RETURN 'OK';


         END; -- end function

-- #END_REGION : FUNCTION FN_DO_DATA_SITE_COVERED
/
