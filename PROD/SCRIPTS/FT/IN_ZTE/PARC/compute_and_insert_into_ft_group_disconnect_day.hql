INSERT INTO MON.FT_GROUP_DISCONNECT_DAY
SELECT
    parc_j
   , parc_pre
   , parc_pos
   , new_pre
   , new_pos
   , parc_j-parc_avant var_parc
   , parc_pre - parc_pre_avant var_parc_pre
   , parc_pos - parc_pos_avant var_parc_post
   , reconn
   , CURRENT_TIMESTAMP INSERTED_DATE
   , a.sdate
FROM (
    SELECT  DATE_SUB(event_date,1) sdate, SUM (effectif) parc_j,
             SUM (CASE WHEN cust_billcycle = 'PURE PREPAID' THEN effectif ELSE 0 END) parc_pre,
             SUM (CASE WHEN cust_billcycle = 'PURE PREPAID' THEN deconnexions ELSE 0 END) res_pre,
             SUM (CASE WHEN cust_billcycle IN ('HYBRID', 'PURE POSTPAID') THEN effectif ELSE 0 END) parc_pos,
             SUM (CASE WHEN cust_billcycle = 'HYBRID' THEN effectif ELSE 0 END) parc_hyb,
             SUM(CASE WHEN cust_billcycle IN ('HYBRID', 'PURE POSTPAID') THEN deconnexions ELSE 0 END) res_pos
        FROM mon.ft_group_subscriber_summary
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
        FROM AGG.ft_a_subscriber_summary e
       WHERE account_status  IN ('ACTIF', 'INACT')
         AND commercial_offer NOT LIKE 'PREPAID SET%'
         AND datecode = DATE_SUB('###SLICE_VALUE###',1)
    GROUP BY datecode
)b,(
    SELECT   event_date sdate, SUM (effectif) parc_avant  ,               
             SUM (CASE WHEN cust_billcycle = 'PURE PREPAID' THEN effectif ELSE 0 END) parc_pre_avant,
             SUM (CASE WHEN cust_billcycle IN ('HYBRID', 'PURE POSTPAID') THEN effectif ELSE 0 END) parc_pos_avant
        FROM mon.ft_group_subscriber_summary
       WHERE operator_code <> 'SET'
         AND (CASE
              WHEN PROFILE IN ('PREPAID PERSO', 'POSTPAID PERSONNELOCM') THEN
                  1
              ELSE 0
            END) = 0
         AND event_date = DATE_SUB('###SLICE_VALUE###',1)
    GROUP BY event_date     
)c, (
    SELECT   DATE_SUB(event_date,1) sdate, COUNT (*) reconn
        FROM MON.ft_account_activity
       WHERE event_date = '###SLICE_VALUE###'
         AND NVL (gp_status, 'ND') = 'ACTIF'
         AND gp_status_date = DATE_SUB('###SLICE_VALUE###',1)
         AND gp_first_active_date <> DATE_SUB('###SLICE_VALUE###',1)
         AND gp_first_active_date IS NOT NULL
    GROUP BY event_date         
)d
WHERE a.sdate = b.sdate and a.sdate = c.sdate and a.sdate = d.sdate
;