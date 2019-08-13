set feedback off
set verify off
SET TERMOUT OFF
set trimspool on
set echo off
set pause off
set pages 10000
set lines 10000
set colsep ";"
set wrap off
SET linesize 999000
--  non affichage de l'entete
SET head off
--
-- forcer le format des dates
ALTER SESSION SET NLS_DATE_FORMAT = 'yyyymmdd hh24miss'; 
-- spool nom_du_fichier.extension
-- spool ###OUTPUT_FILEPATH###
-- ###FLUXLOGICAL_NAME###  , ###SQL_FILENAME_INFIX### , 2019-08-08 ,  ###OUTPUT_FILEPATH###
--
--
-- sample {0} = 20090708
-- Ø
	-- spool nom_du_fichier.extension
	spool ###OUTPUT_FILEPATH###
	--
	--
	-- preciser l'entete
	-- SELECT 'EVENT_DATE, ,' ROW_DATA FROM DUAL ;
	--
	-- || ';' ||
 
SELECT
      'Parc G '|| TO_CHAR (a.sdate , 'dd/mm/yy')
      || CHR(13) || 'Orange '|| parc_j
      || CHR(13) || '-PRE '|| parc_pre
      ||', G Add '|| new_pre
      ||', res.'|| (- parc_pre + parc_pre_avant + new_pre)
      || CHR(13) ||'-POST '|| parc_pos
      ||', dont Hyb '|| parc_hyb
      ||', G Add '|| new_pos
      ||', res.'|| (- parc_pos + parc_pos_avant + new_pos)
      || CHR(13) ||'-recon. Pre et Hyb '|| reconn
	  || CHR(13) ||'Parc ART '|| parc_art
      ||'' ROW_DATA 
FROM (
        SELECT   DATE_SUB(event_date,1) sdate, SUM (effectif) parc_j,
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
             AND event_date = '2019-08-08'
        GROUP BY event_date
    )a,(
        SELECT   datecode sdate,
		         SUM (CASE WHEN network_domain = 'GSM' AND account_status  IN ('ACTIF', 'INACT') THEN TOTAL_COUNT ELSE 0 END) parc_comm,
                 SUM (CASE WHEN network_domain = 'GSM' AND account_status = 'ACTIF' AND NVL (subscriber_type, 'PURE PREPAID') = 'PURE PREPAID' THEN total_activation ELSE 0 END ) new_pre,
                 SUM(CASE WHEN network_domain = 'GSM' AND account_status = 'ACTIF'AND subscriber_type IN ('HYBRID', 'PURE POSTPAID') THEN total_activation ELSE 0 END ) new_pos
            FROM AGG.ft_a_subscriber_summary e
           WHERE account_status  IN ('ACTIF', 'INACT')
             AND commercial_offer NOT LIKE 'PREPAID SET%'
             AND datecode = DATE_SUB('2019-08-08',1)
        GROUP BY datecode
    )b,(
        SELECT   event_date sdate,		         
                 SUM (CASE WHEN cust_billcycle = 'PURE PREPAID' THEN effectif ELSE 0 END) parc_pre_avant,
                 SUM (CASE WHEN cust_billcycle IN ('HYBRID', 'PURE POSTPAID') THEN effectif ELSE 0 END) parc_pos_avant
            FROM mon.ft_group_subscriber_summary
           WHERE operator_code <> 'SET'
             AND (CASE
                    WHEN PROFILE IN ('PREPAID PERSO', 'POSTPAID PERSONNELOCM') THEN
                        1
                    ELSE 0
                  END) = 0
             AND event_date = DATE_SUB('2019-08-08',1)
        GROUP BY event_date     
    )c, (
          SELECT datecode sdate,SUM (total_count) parc_art
            FROM MON.ft_commercial_subscrib_summary
           WHERE datecode = DATE_SUB('2019-08-08',1)
             AND account_status = 'ACTIF'
        GROUP BY datecode           
    )d,(
          SELECT MAX (sdate) sdate, MAX (reconn) reconn
            FROM MON.ft_group_disconnect_day
           WHERE sdate = DATE_SUB('2019-08-08',1)    
    )e
WHERE a.sdate = b.sdate and a.sdate = c.sdate and a.sdate = d.sdate and a.sdate = e.sdate


fn_group_disconnect_day