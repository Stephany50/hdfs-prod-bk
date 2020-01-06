INSERT INTO tmp.TMP_CUST_CONTACT_ORDER
SELECT a.customer_id, custcode, prgcode, ccname, ccline2, ccline3,  cast(ohstatus as string) ohstatus, ohentdate, ohrefnum, balance, billcycle,
    (CASE WHEN b.customer_id IS NOT NULL /*AND ohstatus != 'DP'*/ THEN balance ELSE 0 END) AS balance_prev_period,
    (CASE WHEN b.customer_id IS NOT NULL AND cast(ohstatus as string) != 'DP' THEN balance ELSE 0 END) AS balance_current,
    (CASE WHEN /* b.customer_id IS NOT NULL AND ohstatus = 'IN' AND*/ ohrefnum IS NOT NULL THEN ohentdate ELSE NULL END) AS bill_date
    , '2019-12-23' event_date, current_timestamp insert_date -- ajout par snr
FROM
(
        select custid customer_id, guid, AcctGuid, custseg prgcode, BILL_CYCLE_ID BILLCYCLE, total_bill_amount/100 cscurbalance, custname ccname, firstname ccline2, lastname ccline3
        from cdr.IT_CUST_FULL
        where original_file_date = '2019-12-23' --and AcctGuid is null
            and nvl(total_bill_amount, 0) != 0 --and bill_cycle_id in ('to_define')
) a
LEFT JOIN
(
    select cust_id customer_id, account_number custcode, null ohstatus, bill_date ohentdate, invoice_number ohrefnum, remaining_amount/100 as balance
    from cdr.IT_BILL
    where original_file_date = '2019-12-23'
) b
    ON a.customer_id = b.customer_id

