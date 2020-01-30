CREATE TABLE JUNK.SOURCE_DATA_WITH_DATE AS
SELECT
    source_data,
    datecode
FROM
(select * from junk.source_data)a,
(select datecode from  DIM.DT_DATES where datecode between  '2019-12-01' and '2020-01-29')b


SELECT
    a.datecode,
    a.source_data
from JUNK.SOURCE_DATA_WITH_DATE a
left  join (
    select
        TRANSACTION_DATE,
        source_data
    from agg.spark_ft_global_activity_daily
    where transaction_date between '2019-12-01' and '2020-01-28'
)b on a.datecode=b.TRANSACTION_DATE and a.source_data=b.source_data
where b.source_data is null
order by 1,2

'2019-12-11','2019-12-17','2020-01-10','2020-01-17','2020-01-18','2020-01-19','2020-01-20','2020-01-21','2020-01-22','2020-01-23','2020-01-24','2020-01-25',
1/18/2020
1/19/2020
1/19/2020
1/20/2020
1/21/2020
1/22/2020
1/23/2020
1/24/2020
1/25/2020
20200130043055.log