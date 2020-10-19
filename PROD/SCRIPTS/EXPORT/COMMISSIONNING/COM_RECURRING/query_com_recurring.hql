
SELECT
replace(acc_nbr,';','-') acc_nbr,
event_time,                             
replace(recurring_type,';','-') recurring_type,
cycle_begin_date,                       
cycle_end_date,                          
replace(event_cost_list,';','-') event_cost_list,
replace(acct_id,';','-') acct_id,
replace(event_cost_res_list,';','-') event_cost_res_list,
replace(benefit_bal_list,';','-') benefit_bal_list,
bal_update_date,                        
replace(prod_spec_code,';','-') prod_spec_code,
replace(price_plan_code,';','-') price_plan_code,
replace(original_file_name,';','-') original_file_name,
original_file_date,                           
original_file_size,                             
original_file_line_count,                      
insert_date,                           
event_date,                                  
file_date                          

FROM cdr.spark_it_zte_recurring
WHERE event_date =  '###SLICE_VALUE###'