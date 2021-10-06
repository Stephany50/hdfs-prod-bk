select
if(A.nb_im_dly_prev >= 10 and
B.nb_im_dly = 0 and
C.nb_im_onl >=10,'OK','NOK')
from 
(select count(*) as nb_im_dly_prev 
from MON.SPARK_FT_CLIENT_LAST_IMEI_DAILY
where event_date = date_sub('###SLICE_VALUE###',1)) A,
(select count(*) as nb_im_dly
from MON.SPARK_FT_CLIENT_LAST_IMEI_DAILY
where event_date = '###SLICE_VALUE###') B,
(select count(*) as nb_im_onl
from MON.SPARK_FT_IMEI_ONLINE
where sdate='###SLICE_VALUE###') C