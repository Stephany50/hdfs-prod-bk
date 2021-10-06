select
if(A.nb_hlr >= 10 and B.nb_hlr2 = 0
            ,'OK','NOK')
FROM (select count(*) as nb_hlr from cdr.spark_it_bdi_hlr where original_file_date=date_add('###SLICE_VALUE###',1)) A
,(select count(*) as nb_hlr2 from MON.SPARK_FT_ABONNE_HLR where event_date='###SLICE_VALUE###') B
