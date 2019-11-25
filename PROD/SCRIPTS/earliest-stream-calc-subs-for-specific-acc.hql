 SELECT
   acc_nbr,
   count(*) subscount,
   WINDOW.START START_DATE,
   WINDOW.END END_DATE,   
   WINDOW.END 
 FROM
(SELECT
    ACC_NBR,
    WINDOW
 FROM in_zte_subscription_stream
 where benefit_name ='IPP 3G Jour Sensation 100'

 ) ITSUBSC
GROUP BY
 acc_nbr,
 WINDOW