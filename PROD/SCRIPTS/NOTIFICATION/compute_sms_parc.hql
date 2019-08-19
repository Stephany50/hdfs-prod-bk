INSERT INTO MON.SMS_PARC
SELECT MSISDN,sms,CURRENT_TIMESTAMP INSERT_DATE,sdate FROM(SELECT * FROM  dim.dt_smsnotification_recipient WHERE type='SMSPARCGROUPE' AND actif='YES')A
LEFT JOIN (
  SELECT
        CONCAT(DATE_FORMAT(a.sdate,'dd/MM')
        ,'\n','Parc ART: ',parc_art
        ,'\nParc G: '
        , '\n' , '    -PRE ', parc_pre
        ,', G Add ', new_pre
        ,', res.', (- parc_pre + parc_pre_avant + new_pre)
        , '\n' ,'    -POST ', parc_pos
        ,', dont Hyb ', parc_hyb
        ,', G Add ', new_pos
        ,', res.', (- parc_pos + parc_pos_avant + new_pos)
        , '\n' , '    -Total ', parc_j
        , '\n' , '    -TDLM ', parc_tdlm,', G ADD ',new_pre_tdlm+new_pos_tdlm,', res ',- parc_pre_tdlm + parc_pre_avant_tdlm + new_pre_tdlm- parc_pos_tdlm + parc_pos_avant_tdlm + new_pos_tdlm
        ,' \n' ,'    -%G ADD ',round((new_pre+new_pos - new_pre_tdlm+new_pos_tdlm)*100/(new_pre_tdlm+new_pos_tdlm),1),' %res ',round((- parc_pre + parc_pre_avant + new_pre- parc_pos + parc_pos_avant + new_pos -(- parc_pre_tdlm + parc_pre_avant_tdlm + new_pre_tdlm- parc_pos_tdlm + parc_pos_avant_tdlm + new_pos_tdlm))*100/(- parc_pre_tdlm + parc_pre_avant_tdlm + new_pre_tdlm- parc_pos_tdlm + parc_pos_avant_tdlm + new_pos_tdlm),1)
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
               AND event_date = '2019-06-18'
          GROUP BY event_date
      )a,(
          SELECT   datecode sdate,
               SUM (CASE WHEN network_domain = 'GSM' AND account_status  IN ('ACTIF', 'INACT') THEN TOTAL_COUNT ELSE 0 END) parc_comm,
                   SUM (CASE WHEN network_domain = 'GSM' AND account_status = 'ACTIF' AND NVL (subscriber_type, 'PURE PREPAID') = 'PURE PREPAID' THEN total_activation ELSE 0 END ) new_pre,
                   SUM(CASE WHEN network_domain = 'GSM' AND account_status = 'ACTIF'AND subscriber_type IN ('HYBRID', 'PURE POSTPAID') THEN total_activation ELSE 0 END ) new_pos
              FROM AGG.ft_a_subscriber_summary e
             WHERE account_status  IN ('ACTIF', 'INACT')
               AND commercial_offer NOT LIKE 'PREPAID SET%'
               AND datecode = DATE_SUB('2019-06-18',1)
          GROUP BY datecode
      )b,(
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
               AND event_date = DATE_SUB('2019-06-18',1)
          GROUP BY event_date
      )c,
      ( SELECT   DATE_SUB('2019-06-18',1) sdate, SUM (effectif) parc_tdlm,
                SUM (CASE WHEN cust_billcycle = 'PURE PREPAID' THEN effectif ELSE 0 END) parc_pre_tdlm,
                   SUM (CASE WHEN cust_billcycle IN ('HYBRID', 'PURE POSTPAID') THEN effectif ELSE 0 END) parc_pos_tdlm
              FROM MON.ft_group_subscriber_summary
             WHERE operator_code <> 'SET'
               AND (CASE
                      WHEN PROFILE IN ('PREPAID PERSO', 'POSTPAID PERSONNELOCM') THEN
                          1
                      ELSE 0
                    END) = 0
               AND event_date = '2019-06-18' -- add_months('2019-06-18',-1)
          GROUP BY event_date
      )d,
      ( SELECT   DATE_SUB('2019-06-18',1)  sdate,
                SUM (CASE WHEN cust_billcycle = 'PURE PREPAID' THEN effectif ELSE 0 END) parc_pre_avant_tdlm,
                   SUM (CASE WHEN cust_billcycle IN ('HYBRID', 'PURE POSTPAID') THEN effectif ELSE 0 END) parc_pos_avant_tdlm
              FROM MON.ft_group_subscriber_summary
             WHERE operator_code <> 'SET'
               AND (CASE
                      WHEN PROFILE IN ('PREPAID PERSO', 'POSTPAID PERSONNELOCM') THEN
                          1
                      ELSE 0
                    END) = 0
               AND event_date =  DATE_SUB('2019-06-18',1) --add_months( DATE_SUB('2019-06-18',1),-1)
          GROUP BY event_date
      )e
      ,(
            SELECT datecode sdate,SUM (total_count) parc_art
              FROM mon.ft_commercial_subscrib_summary
             WHERE datecode = DATE_SUB('2019-06-18',1)
               AND account_status = 'ACTIF'
          GROUP BY datecode
      )f
        ,(SELECT   DATE_SUB('2019-06-18',1) sdate,
                   SUM (CASE WHEN network_domain = 'GSM' AND account_status = 'ACTIF' AND NVL (subscriber_type, 'PURE PREPAID') = 'PURE PREPAID' THEN total_activation ELSE 0 END ) new_pre_tdlm,
                   SUM(CASE WHEN network_domain = 'GSM' AND account_status = 'ACTIF'AND subscriber_type IN ('HYBRID', 'PURE POSTPAID') THEN total_activation ELSE 0 END ) new_pos_tdlm
              FROM AGG.ft_a_subscriber_summary e
             WHERE account_status  IN ('ACTIF', 'INACT')
               AND commercial_offer NOT LIKE 'PREPAID SET%'
               AND  datecode =DATE_SUB('2019-06-18',1)-- add_months(DATE_SUB('2019-06-18',1),-1)
          GROUP BY datecode
          )g
  WHERE a.sdate = b.sdate and a.sdate = c.sdate  and a.sdate = d.sdate
)B