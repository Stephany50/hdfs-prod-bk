--INSERT INTO MON.SMS_PARC
SELECT 
    MSISDN,
    sms,
    CURRENT_TIMESTAMP INSERT_DATE,
    sdate 
FROM(
    SELECT * 
    FROM  dim.dt_smsnotification_recipient 
    WHERE type='SMSPARCGROUPE' AND actif='YES'
)A
LEFT JOIN (
  SELECT
        CONCAT(DATE_FORMAT(a.sdate,'dd/MM')
        ,'\nParc G: ',parc_j
        , '\n' , '    -PRE: ', parc_pre
        ,', G Add ', new_pre
        ,', res', (- parc_pre + parc_pre_avant + new_pre)
        , '\n' ,'    -POST: ', parc_pos
        ,', dont Hyb ', parc_hyb
        ,', G Add ', new_pos
        ,', res', (- parc_pos + parc_pos_avant + new_pos)
        , '\n' , '    -MTD:  G ADD  ', new_mtd, ' res ',(- parc_pre -parc_pos + parc_01 + new_mtd)
        , '\n' , '    -LMTD: G ADD  ', new_lmtd, ' res ',(- parc_tdlm + parc_01lm + new_lmtd)
        , '\n' , '    -%: G ADD  ', round((new_mtd - new_lmtd)*100/new_lmtd,1), ' res ',round(((- parc_pre -parc_pos + parc_01 + new_mtd) - (- parc_tdlm + parc_01lm + new_lmtd))*100/(- parc_tdlm + parc_01lm + new_lmtd),1)
        ,'\n','Parc ART: ',parc_art
         ) sms
         ,a.sdate
  FROM (
        SELECT   DATE_SUB(event_date,1) sdate, SUM (effectif) parc_j,
                   SUM (CASE WHEN cust_billcycle = 'PURE PREPAID' THEN effectif ELSE 0 END) parc_pre,
                   SUM (CASE WHEN cust_billcycle = 'PURE PREPAID' THEN deconnexions ELSE 0 END) res_pre,
                   SUM (CASE WHEN cust_billcycle IN ('HYBRID', 'PURE POSTPAID') THEN effectif ELSE 0 END) parc_pos,
                   SUM (CASE WHEN cust_billcycle = 'HYBRID' THEN effectif ELSE 0 END) parc_hyb,
                   SUM(CASE WHEN cust_billcycle IN ('HYBRID', 'PURE POSTPAID') THEN deconnexions ELSE 0 END) res_pos
              FROM MON.ft_group_subscriber_summary
             WHERE operator_code <> 'SET'
               AND (CASE
                      WHEN PROFILE IN ('PREPAID PERSO', 'POSTPAID PERSONNELOCM') THEN
                          1
                      ELSE 0
                    END) = 0
               AND event_date = '###SLICE_VALUE###'
          GROUP BY event_date
      )a,(
          SELECT   datecode sdate,
               SUM (CASE WHEN network_domain = 'GSM' AND account_status  IN ('ACTIF', 'INACT') THEN TOTAL_COUNT ELSE 0 END) parc_comm,
                   SUM (CASE WHEN network_domain = 'GSM' AND account_status = 'ACTIF' AND NVL (subscriber_type, 'PURE PREPAID') = 'PURE PREPAID' THEN total_activation ELSE 0 END ) new_pre,
                   SUM(CASE WHEN network_domain = 'GSM' AND account_status = 'ACTIF'AND subscriber_type IN ('HYBRID', 'PURE POSTPAID') THEN total_activation ELSE 0 END ) new_pos
              FROM agg.ft_a_subscriber_summary e
             WHERE account_status  IN ('ACTIF', 'INACT')
               AND commercial_offer NOT LIKE 'PREPAID SET%'
               AND datecode = DATE_SUB('###SLICE_VALUE###',1)
          GROUP BY datecode
      )b,
      (
          SELECT   event_date sdate,
                   SUM (CASE WHEN cust_billcycle = 'PURE PREPAID' THEN effectif ELSE 0 END) parc_pre_avant,
                   SUM (CASE WHEN cust_billcycle IN ('HYBRID', 'PURE POSTPAID') THEN effectif ELSE 0 END) parc_pos_avant
              FROM MON.ft_group_subscriber_summary
             WHERE operator_code <> 'SET'
               AND (CASE
                      WHEN PROFILE IN ('PREPAID PERSO', 'POSTPAID PERSONNELOCM') THEN
                          1
                      ELSE 0
                    END) = 0
               AND event_date = DATE_SUB('###SLICE_VALUE###',1)
          GROUP BY event_date
      )c,
      (
            SELECT datecode sdate,SUM (total_count) parc_art
              FROM mon.ft_commercial_subscrib_summary
             WHERE datecode = DATE_SUB('###SLICE_VALUE###',1)
               AND account_status = 'ACTIF'
          GROUP BY datecode
      )d,
      (
          SELECT   DATE_SUB('###SLICE_VALUE###',1)  sdate,
                   SUM (CASE WHEN cust_billcycle in ('HYBRID', 'PURE POSTPAID','PURE PREPAID') THEN effectif ELSE 0 END) parc_tdlm
              FROM MON.ft_group_subscriber_summary
             WHERE operator_code <> 'SET'
               AND (CASE
                      WHEN PROFILE IN ('PREPAID PERSO', 'POSTPAID PERSONNELOCM') THEN
                          1
                      ELSE 0
                    END) = 0
               AND event_date = add_months('###SLICE_VALUE###',-1)
          GROUP BY event_date
      )e
      ,(
          SELECT   DATE_SUB('###SLICE_VALUE###',1) sdate,
                   SUM (CASE WHEN cust_billcycle IN ('HYBRID', 'PURE POSTPAID','PURE PREPAID') THEN effectif ELSE 0 END) parc_01lm
              FROM MON.ft_group_subscriber_summary
             WHERE operator_code <> 'SET'
               AND (CASE
                      WHEN PROFILE IN ('PREPAID PERSO', 'POSTPAID PERSONNELOCM') THEN
                          1
                      ELSE 0
                    END) = 0
               AND event_date = add_months(CONCAT(SUBSTRING('###SLICE_VALUE###',0,7),'-','01'),-1)
          GROUP BY event_date
      )f
      ,(
          SELECT   DATE_SUB('###SLICE_VALUE###',1) sdate,
                   SUM (CASE WHEN cust_billcycle IN ('HYBRID', 'PURE POSTPAID','PURE PREPAID') THEN effectif ELSE 0 END) parc_01
              FROM MON.ft_group_subscriber_summary
             WHERE operator_code <> 'SET'
               AND (CASE
                      WHEN PROFILE IN ('PREPAID PERSO', 'POSTPAID PERSONNELOCM') THEN
                          1
                      ELSE 0
                    END) = 0
               AND event_date = CONCAT(SUBSTRING('###SLICE_VALUE###',0,7),'-','01')
          GROUP BY event_date
      )i,
      (
          SELECT   DATE_SUB('###SLICE_VALUE###',1) sdate,
                   SUM (CASE WHEN network_domain = 'GSM' AND account_status = 'ACTIF' AND NVL (subscriber_type, 'PURE PREPAID') IN ('HYBRID', 'PURE POSTPAID','PURE PREPAID')  THEN total_activation ELSE 0 END ) new_mtd
                   FROM agg.ft_a_subscriber_summary e
             WHERE account_status  IN ('ACTIF', 'INACT')
               AND commercial_offer NOT LIKE 'PREPAID SET%'
               AND datecode between  CONCAT(SUBSTRING(DATE_SUB('###SLICE_VALUE###',1),0,7),'-','01') and DATE_SUB('###SLICE_VALUE###',1)
      )j,
       (
          SELECT   DATE_SUB('###SLICE_VALUE###',1) sdate,
                   SUM (CASE WHEN network_domain = 'GSM' AND account_status = 'ACTIF' AND NVL (subscriber_type, 'PURE PREPAID') IN ('HYBRID', 'PURE POSTPAID','PURE PREPAID') THEN total_activation ELSE 0 END ) new_lmtd
                   FROM agg.ft_a_subscriber_summary e
             WHERE account_status  IN ('ACTIF', 'INACT')
               AND commercial_offer NOT LIKE 'PREPAID SET%'
               AND datecode between  add_months(CONCAT(SUBSTRING(DATE_SUB('###SLICE_VALUE###',1),0,7),'-','01'),-1)  and add_months('###SLICE_VALUE###',-1)
      )k
      WHERE a.sdate = b.sdate and a.sdate = c.sdate  and a.sdate = d.sdate and a.sdate = e.sdate and a.sdate = f.sdate and a.sdate = i.sdate and a.sdate = j.sdate and a.sdate = k.sdate
)B