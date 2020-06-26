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