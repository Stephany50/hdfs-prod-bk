
SELECT
replace(nvl(acc_nbr,''),';','-') acc_nbr,
nvl(to_date(event_time),'') event_time,                             
replace(nvl(recurring_type,''),';','-') recurring_type,
nvl(cycle_begin_date,'') cycle_begin_date,                       
nvl(cycle_end_date,'') cycle_end_date,                          
replace(nvl(event_cost_list,''),';','-') event_cost_list,
replace(nvl(acct_id,''),';','-') acct_id,
replace(nvl(event_cost_res_list,''),';','-') event_cost_res_list,
replace(nvl(benefit_bal_list,''),';','-') benefit_bal_list,
nvl(to_date(bal_update_date),'') bal_update_date,                        
replace(nvl(prod_spec_code,''),';','-') prod_spec_code,
replace(nvl(price_plan_code,''),';','-') price_plan_code,
replace(nvl(original_file_name,''),';','-') original_file_name,
nvl(to_date(original_file_date),'') original_file_date,                           
nvl(original_file_size,'') original_file_size,                             
nvl(original_file_line_count,'') original_file_line_count,                      
nvl(to_date(insert_date),'') insert_date,                           
nvl(to_date(event_date),'') event_date,                                  
nvl(to_date(file_date),'') file_date                         
FROM cdr.spark_it_zte_recurring
WHERE event_date =  '###SLICE_VALUE###'