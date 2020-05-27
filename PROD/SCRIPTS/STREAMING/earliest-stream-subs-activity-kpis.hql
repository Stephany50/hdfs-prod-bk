SELECT
UNIX_TIMESTAMP(window.start) DIV 300 * 300  time,
sum(subs_amount)  Amount,
sum(subs_total_count)  nbr_total,
sum(case when subs_amount <> 0 then subs_total_count ELSE 0 END)  nbr_fact,
sum(case when subs_amount = 0 then subs_total_count ELSE 0 END)  nbr_nonfact
FROM subs_activity
group by UNIX_TIMESTAMP(window.start) DIV 300 * 300