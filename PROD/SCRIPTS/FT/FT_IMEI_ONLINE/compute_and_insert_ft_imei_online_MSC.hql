MERGE INTO MON.FT_IMEI_ONLINE  D
                USING
                (
                    SELECT

					IMEI,
					IMSI,
					MSISDN,
					NBRE ,
					current_timestamp as INSERT_DATE,
					SDATE
						FROM
						(
							SELECT

							SERVED_IMEI IMEI,
							SERVED_IMSI IMSI,
							SERVED_MSISDN MSISDN,
							SUM(1) NBRE,
							current_timestamp as INSERT_DATE,
							TRANSACTION_DATE SDATE
							FROM MON.FT_MSC_TRANSACTION
							WHERE
								  TRANSACTION_DATE = '2019-08-07'
								  AND SERVED_IMEI IS NOT NULL
							GROUP BY
							TRANSACTION_DATE,
							SERVED_IMEI,
							SERVED_IMSI,
							SERVED_MSISDN
						)
                ) S
               on (
			        D.SDATE = S.SDATE
                    AND D.IMEI = S.IMEI
                    AND D.IMSI = S.IMSI
                    AND D.MSISDN = S.MSISDN
                    )
                when matched then
                    update set D.src_table = concat(D.src_table,'MSC|')
                when not matched then
                    insert (D.IMEI, D.IMSI, D.MSISDN, D.SRC_TABLE, TRANSACTION_COUNT, D.INSERT_DATE, D.SDATE)
                        values(S.IMEI, S.IMSI, S.MSISDN, 'MSC|', NBRE,current_timestamp, S.SDATE)