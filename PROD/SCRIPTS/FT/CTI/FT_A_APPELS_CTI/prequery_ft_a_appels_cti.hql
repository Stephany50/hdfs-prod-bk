select if(it_exist>25000 and ft_exist=0 , 'OK', 'NOK') val from
( select count(*) it_exist from CTI.IT_IRF_USER_DATA_CUST where event_date = '###SLICE_VALUE###' ) a,
( select count(*) ft_exist from AGG.FT_A_APPELS_CTI where event_date = '###SLICE_VALUE###' ) b

