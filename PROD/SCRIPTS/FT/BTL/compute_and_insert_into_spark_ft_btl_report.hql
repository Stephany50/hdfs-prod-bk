insert into mon.spark_ft_btl_report
select 
    msisdn,
    aa.type_forfait,
    msisdn_vendeur,
    aa.prix,
    ipp,
    ipp_name,
    case 
        when nvl(coeff_onnet, 0)+nvl(coeff_offnet, 0)+nvl(coeff_inter, 0)+nvl(coef_sms, 0)+nvl(coeff_roaming_voix, 0)+nvl(coeff_roaming_sms, 0) = 100 then 'VOIX_ONLY'
        when nvl(coeff_data, 0)+nvl(coeff_roaming_data, 0) = 100 then 'DATA_ONLY'
        when nvl(coeff_onnet, 0)+nvl(coeff_offnet, 0)+nvl(coeff_inter, 0)+nvl(coef_sms, 0)+nvl(coeff_roaming_voix, 0)+nvl(coeff_roaming_sms, 0) < 100 or nvl(coeff_data, 0)+nvl(coeff_roaming_data, 0) < 100 then 'COMBO'
    end ipp_stream,
    current_timestamp insert_date,
    '###SLICE_VALUE###' transaction_date
from
(
    select a.* ,ipp_name
    from 
    (
        select * from cdr.spark_it_btl_report where original_file_date = '###SLICE_VALUE###'
    ) a 
    --left join dim.ref_souscription_price b 
    left join (select distinct ipp_name, ipp_code from CDR.SPARK_IT_ZTE_IPP_EXTRACT where original_file_date = (select max(original_file_date) from CDR.SPARK_IT_ZTE_IPP_EXTRACT where original_file_date >= date_sub('###SLICE_VALUE###', 7))) b
    on upper(trim(cast(a.ipp as varchar(200)))) = upper(trim(cast(b.ipp_code as varchar(200))))
) aa 
left join
(
    select distinct * from dim.dt_cbm_ref_souscription_price
) bb  
on upper(trim(ipp_name)) = upper(trim(bdle_name))