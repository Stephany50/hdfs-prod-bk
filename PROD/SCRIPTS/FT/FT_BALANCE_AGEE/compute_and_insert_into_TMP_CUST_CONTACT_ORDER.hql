insert into tmp.TMP_CUST_CONTACT_ORDER
SELECT a.customer_id, custcode, prgcode, ccname, ccline2, ccline3,  cast(ohstatus as string) ohstatus, ohentdate, ohrefnum, balance, billcycle,
    (CASE WHEN b.customer_id IS NOT NULL THEN balance ELSE 0 END) AS balance_prev_period,
    balance balance_current,
    (CASE WHEN ohrefnum IS NOT NULL THEN ohentdate ELSE NULL END) AS bill_date
    , a.original_file_date event_date, current_timestamp insert_date
FROM
(
        select original_file_date, custid customer_id, guid, custseg prgcode, max(BILL_CYCLE_ID) BILLCYCLE, sum(total_bill_amount/100) cscurbalance, custname ccname, firstname ccline2, lastname ccline3
        from cdr.IT_CUST_FULL
        where original_file_date = '2020-01-01' --and AcctGuid is null
            and nvl(total_bill_amount, 0) != 0 --and bill_cycle_id in ('to_define')
        group by original_file_date, custid, guid, custseg, custname, firstname, lastname
) a LEFT JOIN (
    select distinct original_file_date, cust_id customer_id, b.account_number custcode, null ohstatus, b.bill_date ohentdate, b.invoice_number ohrefnum, b.remaining_amount/100 as balance
    from cdr.IT_BILL b
    where original_file_date = '2020-01-01'
) b  ON a.customer_id = b.customer_id and a.original_file_date = b.original_file_date ;