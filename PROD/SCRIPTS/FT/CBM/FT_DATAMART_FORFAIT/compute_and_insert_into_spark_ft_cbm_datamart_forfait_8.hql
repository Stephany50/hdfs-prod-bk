create table tt.spark_cbm_data_2 as select * from tt.spark_cbm_data_2_backup where type <> 'Combo'