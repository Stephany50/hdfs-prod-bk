MERGE INTO TMP.TT_FT_IMEI_ONLINE  D

                USING
                (

							SELECT

							SERVED_IMEI IMEI,
							SERVED_IMSI IMSI,
							SERVED_MSISDN MSISDN,
							SUM(1) NBRE,
							TRANSACTION_DATE SDATE

							FROM MON.FT_MSC_TRANSACTION
							WHERE
								  TRANSACTION_DATE = '###SLICE_VALUE###'
								  AND SERVED_IMEI IS NOT NULL
							GROUP BY
							TRANSACTION_DATE,
							SERVED_IMEI,
							SERVED_IMSI,
							SERVED_MSISDN

                ) S
              on (
			        D.SDATE = S.SDATE
                    AND D.IMEI = S.IMEI
                    AND D.IMSI = S.IMSI
                    AND D.MSISDN = S.MSISDN
                    )
                when matched then
                    update set SRC_TABLE = concat(SRC_TABLE,'MSC|')
                when not matched then
                    insert
                        VALUES (S.IMEI, S.IMSI, S.MSISDN, NBRE, 'MSC|', CURRENT_TIMESTAMP, S.SDATE)