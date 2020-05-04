
    -------- Recharges (C2S, CAG, OM) ----------------
select 
    CURRENT_DATE processing_date,
    region_administrative,
    region_commerciale,
    'Distribution' category,
    'Recharges (C2S, CAG, OM)' kpi, 
    'FT_REFILL|FT_SUBSCRIPTION' source_table,
    sum(valeur) valeur),
    sum(valeur_lweek) valeur_lweek,
    sum(valeur_4wa) valeur_4wa,
    sum(valeur_mtd) valeur_mtd,
    sum(valeur_lmtd) valeur_lmtd,
    sum(valeur_mtd_vs_lmdt) valeur_mtd_vs_lmdt,
    sum(valeur_mtd_last_year) valeur_mtd_last_year,
    sum(mdt_vs_last_year) mdt_vs_last_year,
    null  valeur_mtd_vs_budget,
    sysdate insert_date
from (
    ---------  HEBDO 
    
    select 
        hebdo.region_administrative
        hebdo.region_commerciale,
        valeur,
        valeur_lweek,
        valeur_4wa,
        valeur_mtd,
        valeur_lmtd,
        round((valeur_mtd-valeur_lmtd)/valeur_lmtd*100,2)valeur_mtd_vs_lmdt,
        round((valeur_mtd-valeur_mtd_last_year)/valeur_mtd_last_year*100,2)valeur_mtd_last_year,
        valeur_mtd_last_year
    from(
        
        select 
             region_administrative,
             region_commerciale,
            sum(valeur) valeur
        from( 
            ------- C2S
            SELECT 
                null region_administrative,
                null region_commerciale,
                SUM(REFILL_AMOUNT) valeur
            FROM  MON.FT_REFILL  
            WHERE refill_date between to_date('06/04/2020','DD/MM/YY') and  to_date('12/04/2020','DD/MM/YY')  AND 
            REFILL_TYPE  IN ('PVAS', 'RC', 'REFILL') and REFILL_MEAN IN ('C2S', 'SCRATCH') AND
            SENDER_CATEGORY in ('TN','TNT', 'WHA', 'ODSA','ODS', 'PS','PT','POS', 'INHSM','INSM','NPOS','ORNGPTNR','PPOS')
            union  all
            select
                null region_administrative,
                null region_commerciale,
                sum(rated_amount) valeur 
            from mon.ft_subscription where transaction_date  between to_date('06/04/2020','DD/MM/YY') and  to_date('12/04/2020','DD/MM/YY')  and 
            AMOUNT_VIA_OM>0 and
            rated_amount>0 and 
            subscription_channel = '32'
            union   all
                ---CAG
            SELECT 
                null region_administrative,
                null region_commerciale,
                sum(REFILL_AMOUNT) REFILL_AMOUNT
            FROM  MON.FT_REFILL  
            WHERE refill_date  between to_date('06/04/2020','DD/MM/YY') and  to_date('12/04/2020','DD/MM/YY') AND 
            TERMINATION_IND='200' AND 
            REFILL_MEAN ='SCRATCH' AND 
            REFILL_TYPE  ='REFILL'
            
            union  all
            ---OM
            SELECT 
               null region_administrative,
                null region_commerciale,
                sum(REFILL_AMOUNT) REFILL_AMOUNT
            FROM  MON.FT_REFILL  
            WHERE refill_date  between to_date('06/04/2020','DD/MM/YY') and  to_date('12/04/2020','DD/MM/YY') AND 
            TERMINATION_IND='200' AND REFILL_MEAN ='C2S' AND
            REFILL_TYPE  ='RC' AND 
            SENDER_CATEGORY IN ('TNT', 'TN')
        )hebdo
        group by region_administrative,region_commerciale
    )hebdo
    left join (
    
        --------- LWEEK
        
        select 
             region_administrative,
             region_commerciale,
            sum(valeur) valeur_lweek
        from( 
            ------- C2S
            SELECT 
                null region_administrative,
                null region_commerciale,
                SUM(REFILL_AMOUNT) valeur
            FROM  MON.FT_REFILL  
            WHERE refill_date between to_date('06/04/2020','DD/MM/YY')-7 and  to_date('12/04/2020','DD/MM/YY')-7  AND 
            REFILL_TYPE  IN ('PVAS', 'RC', 'REFILL') and REFILL_MEAN IN ('C2S', 'SCRATCH') AND
            SENDER_CATEGORY in ('TN','TNT', 'WHA', 'ODSA','ODS', 'PS','PT','POS', 'INHSM','INSM','NPOS','ORNGPTNR','PPOS')
            union  all
            select
                null region_administrative,
                null region_commerciale,
                sum(rated_amount) valeur 
            from mon.ft_subscription where transaction_date  between to_date('06/04/2020','DD/MM/YY')-7 and  to_date('12/04/2020','DD/MM/YY')-7  and 
            AMOUNT_VIA_OM>0 and
            rated_amount>0 and 
            subscription_channel = '32'
            union   all
                ---CAG
            SELECT 
                null region_administrative,
                null region_commerciale,
                sum(REFILL_AMOUNT) REFILL_AMOUNT
            FROM  MON.FT_REFILL  
            WHERE refill_date  between to_date('06/04/2020','DD/MM/YY')-7 and  to_date('12/04/2020','DD/MM/YY')-7 AND 
            TERMINATION_IND='200' AND 
            REFILL_MEAN ='SCRATCH' AND 
            REFILL_TYPE  ='REFILL'
            
            union  all
            ---OM
            SELECT 
               null region_administrative,
                null region_commerciale,
                sum(REFILL_AMOUNT) REFILL_AMOUNT
            FROM  MON.FT_REFILL  
            WHERE refill_date  between to_date('06/04/2020','DD/MM/YY')-7 and  to_date('12/04/2020','DD/MM/YY')-7 AND 
            TERMINATION_IND='200' AND REFILL_MEAN ='C2S' AND
            REFILL_TYPE  ='RC' AND 
            SENDER_CATEGORY IN ('TNT', 'TN')
        )lweek
        group by region_administrative,region_commerciale
    )lweek on nvl(hebdo.region_commerciale,'ND')=nvl(lweek.region_commerciale,'ND')
    left join (
        
        
        ---- 4WA
        select 
             region_administrative,
             region_commerciale,
            sum(valeur) valeur_4wa
        from( 
            ------- C2S
            SELECT 
                null region_administrative,
                null region_commerciale,
                SUM(REFILL_AMOUNT) valeur
            FROM  MON.FT_REFILL  
            WHERE refill_date between to_date('06/04/2020','DD/MM/YY')-28 and  to_date('12/04/2020','DD/MM/YY')-28  AND 
            REFILL_TYPE  IN ('PVAS', 'RC', 'REFILL') and REFILL_MEAN IN ('C2S', 'SCRATCH') AND
            SENDER_CATEGORY in ('TN','TNT', 'WHA', 'ODSA','ODS', 'PS','PT','POS', 'INHSM','INSM','NPOS','ORNGPTNR','PPOS')
            union  all
            select
                null region_administrative,
                null region_commerciale,
                sum(rated_amount) valeur 
            from mon.ft_subscription where transaction_date  between to_date('06/04/2020','DD/MM/YY')-28 and  to_date('12/04/2020','DD/MM/YY')-28  and 
            AMOUNT_VIA_OM>0 and
            rated_amount>0 and 
            subscription_channel = '32'
            union   all
                ---CAG
            SELECT 
                null region_administrative,
                null region_commerciale,
                sum(REFILL_AMOUNT) REFILL_AMOUNT
            FROM  MON.FT_REFILL  
            WHERE refill_date  between to_date('06/04/2020','DD/MM/YY')-28 and  to_date('12/04/2020','DD/MM/YY')-28 AND 
            TERMINATION_IND='200' AND 
            REFILL_MEAN ='SCRATCH' AND 
            REFILL_TYPE  ='REFILL'
            
            union  all
            ---OM
            SELECT 
               null region_administrative,
                null region_commerciale,
                sum(REFILL_AMOUNT) REFILL_AMOUNT
            FROM  MON.FT_REFILL  
            WHERE refill_date  between to_date('06/04/2020','DD/MM/YY')-28 and  to_date('12/04/2020','DD/MM/YY')-28 AND 
            TERMINATION_IND='200' AND REFILL_MEAN ='C2S' AND
            REFILL_TYPE  ='RC' AND 
            SENDER_CATEGORY IN ('TNT', 'TN')
        )4wa
        group by region_administrative,region_commerciale
    )4wa on nvl(hebdo.region_commerciale,'ND')=nvl(4wa.region_commerciale,'ND')
    
    left join (
    -----MTD
        select 
             region_administrative,
             region_commerciale,
            sum(valeur) valeur_mtd
        from( 
            ------- C2S
            SELECT 
                null region_administrative,
                null region_commerciale,
                SUM(REFILL_AMOUNT) valeur
            FROM  MON.FT_REFILL  
            WHERE refill_date between ADD_MONTHS((LAST_DAY(to_date('12/04/2020','DD/MM/YY'))+1),-1)  and  to_date('12/04/2020','DD/MM/YY')  AND 
            REFILL_TYPE  IN ('PVAS', 'RC', 'REFILL') and REFILL_MEAN IN ('C2S', 'SCRATCH') AND
            SENDER_CATEGORY in ('TN','TNT', 'WHA', 'ODSA','ODS', 'PS','PT','POS', 'INHSM','INSM','NPOS','ORNGPTNR','PPOS')
            union  all
            select
                null region_administrative,
                null region_commerciale,
                sum(rated_amount) valeur 
            from mon.ft_subscription where transaction_date  between ADD_MONTHS((LAST_DAY(to_date('12/04/2020','DD/MM/YY'))+1),-1)  and  to_date('12/04/2020','DD/MM/YY')  and 
            AMOUNT_VIA_OM>0 and
            rated_amount>0 and 
            subscription_channel = '32'
            union   all
                ---CAG
            SELECT 
                null region_administrative,
                null region_commerciale,
                sum(REFILL_AMOUNT) REFILL_AMOUNT
            FROM  MON.FT_REFILL  
            WHERE refill_date  between ADD_MONTHS((LAST_DAY(to_date('12/04/2020','DD/MM/YY'))+1),-1)  and  to_date('12/04/2020','DD/MM/YY') AND 
            TERMINATION_IND='200' AND 
            REFILL_MEAN ='SCRATCH' AND 
            REFILL_TYPE  ='REFILL'
            
            union  all
            ---OM
            SELECT 
               null region_administrative,
                null region_commerciale,
                sum(REFILL_AMOUNT) REFILL_AMOUNT
            FROM  MON.FT_REFILL  
            WHERE refill_date  between ADD_MONTHS((LAST_DAY(to_date('12/04/2020','DD/MM/YY'))+1),-1)  and  to_date('12/04/2020','DD/MM/YY') AND 
            TERMINATION_IND='200' AND REFILL_MEAN ='C2S' AND
            REFILL_TYPE  ='RC' AND 
            SENDER_CATEGORY IN ('TNT', 'TN')
        )mtd
        group by region_administrative,region_commerciale
    )mtd on nvl(hebdo.region_commerciale,'ND')=nvl(mtd.region_commerciale,'ND')
    
    left join (
        --LMTD
        select 
             region_administrative,
             region_commerciale,
            sum(valeur) valeur_lmtd
        from( 
            ------- C2S
            SELECT 
                null region_administrative,
                null region_commerciale,
                SUM(REFILL_AMOUNT) valeur
            FROM  MON.FT_REFILL  
            WHERE refill_date between  ADD_MONTHS((LAST_DAY(to_date('12/04/2020','DD/MM/YY'))+1),-2)  and  ADD_MONTHS(to_date('12/04/2020','DD/MM/YY'),-1)  AND 
            REFILL_TYPE  IN ('PVAS', 'RC', 'REFILL') and REFILL_MEAN IN ('C2S', 'SCRATCH') AND
            SENDER_CATEGORY in ('TN','TNT', 'WHA', 'ODSA','ODS', 'PS','PT','POS', 'INHSM','INSM','NPOS','ORNGPTNR','PPOS')
            union  all
            select
                null region_administrative,
                null region_commerciale,
                sum(rated_amount) valeur 
            from mon.ft_subscription where transaction_date  between  ADD_MONTHS((LAST_DAY(to_date('12/04/2020','DD/MM/YY'))+1),-2)  and  ADD_MONTHS(to_date('12/04/2020','DD/MM/YY'),-1)  and 
            AMOUNT_VIA_OM>0 and
            rated_amount>0 and 
            subscription_channel = '32'
            union   all
                ---CAG
            SELECT 
                null region_administrative,
                null region_commerciale,
                sum(REFILL_AMOUNT) REFILL_AMOUNT
            FROM  MON.FT_REFILL  
            WHERE refill_date  between  ADD_MONTHS((LAST_DAY(to_date('12/04/2020','DD/MM/YY'))+1),-2)  and  ADD_MONTHS(to_date('12/04/2020','DD/MM/YY'),-1) AND 
            TERMINATION_IND='200' AND 
            REFILL_MEAN ='SCRATCH' AND 
            REFILL_TYPE  ='REFILL'
            
            union  all
            ---OM
            SELECT 
               null region_administrative,
                null region_commerciale,
                sum(REFILL_AMOUNT) REFILL_AMOUNT
            FROM  MON.FT_REFILL  
            WHERE refill_date  between  ADD_MONTHS((LAST_DAY(to_date('12/04/2020','DD/MM/YY'))+1),-2)  and  ADD_MONTHS(to_date('12/04/2020','DD/MM/YY'),-1) AND
            TERMINATION_IND='200' AND REFILL_MEAN ='C2S' AND
            REFILL_TYPE  ='RC' AND 
            SENDER_CATEGORY IN ('TNT', 'TN')
        )lmtd
        group by region_administrative,region_commerciale
    )lmtd on nvl(hebdo.region_commerciale,'ND')=nvl(lweek.lmtd,'ND')
    
    left join (
        
       --MTD LAST YEAR
        select 
             region_administrative,
             region_commerciale,
            sum(valeur) valeur_mtd_last_year
        from( 
            ------- C2S
            SELECT 
                null region_administrative,
                null region_commerciale,
                SUM(REFILL_AMOUNT) valeur
            FROM  MON.FT_REFILL  
            WHERE refill_date between  ADD_MONTHS((LAST_DAY(to_date('12/04/2020','DD/MM/YY'))+1),-13)  and  ADD_MONTHS(to_date('12/04/2020','DD/MM/YY'),-12) AND 
            REFILL_TYPE  IN ('PVAS', 'RC', 'REFILL') and REFILL_MEAN IN ('C2S', 'SCRATCH') AND
            SENDER_CATEGORY in ('TN','TNT', 'WHA', 'ODSA','ODS', 'PS','PT','POS', 'INHSM','INSM','NPOS','ORNGPTNR','PPOS')
            union  all
            select
                null region_administrative,
                null region_commerciale,
                sum(rated_amount) valeur 
            from mon.ft_subscription where transaction_date  between  ADD_MONTHS((LAST_DAY(to_date('12/04/2020','DD/MM/YY'))+1),-13)  and  ADD_MONTHS(to_date('12/04/2020','DD/MM/YY'),-12)  and 
            AMOUNT_VIA_OM>0 and
            rated_amount>0 and 
            subscription_channel = '32'
            union   all
                ---CAG
            SELECT 
                null region_administrative,
                null region_commerciale,
                sum(REFILL_AMOUNT) REFILL_AMOUNT
            FROM  MON.FT_REFILL  
            WHERE refill_date  between  ADD_MONTHS((LAST_DAY(to_date('12/04/2020','DD/MM/YY'))+1),-13)  and  ADD_MONTHS(to_date('12/04/2020','DD/MM/YY'),-12) AND 
            TERMINATION_IND='200' AND 
            REFILL_MEAN ='SCRATCH' AND 
            REFILL_TYPE  ='REFILL'
            
            union  all
            ---OM
            SELECT 
               null region_administrative,
                null region_commerciale,
                sum(REFILL_AMOUNT) REFILL_AMOUNT
            FROM  MON.FT_REFILL  
            WHERE refill_date  between  ADD_MONTHS((LAST_DAY(to_date('12/04/2020','DD/MM/YY'))+1),-13)  and  ADD_MONTHS(to_date('12/04/2020','DD/MM/YY'),-12) AND 
            TERMINATION_IND='200' AND REFILL_MEAN ='C2S' AND
            REFILL_TYPE  ='RC' AND 
            SENDER_CATEGORY IN ('TNT', 'TN')
        )mtd_last_year
        group by region_administrative,region_commerciale
    )mtd_last_year on nvl(hebdo.region_commerciale,'ND')=nvl(mtd_last_year.region_commerciale,'ND')
    
)
group by refill_date 
