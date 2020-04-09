DROP TABLE IF EXISTS rates_temp;
CREATE TABLE rates_temp AS
(SELECT DISTINCT *,
       ROW_NUMBER() OVER (PARTITION BY session_id,channel_name ORDER BY fetched_at,rate DESC) AS rn
FROM titan.rates AS  rates
--WHERE rates.session_id='-gGlVseIV3oSbgsV2mCIGKcT'
GROUP BY session_id,website_hash,channel_name,rate,product,currency_code,arrival_date,departure_date,fetched_at,cached
);

DROP TABLE IF EXISTS rates_latest_temp;
CREATE TABLE rates_latest_temp AS
(SELECT rates_temp.*,
       session.utc_time,
       CASE WHEN rates_temp.currency_code = 'EUR' THEN rates_temp.rate
            ELSE rates_temp.rate/currency_exchange_rates.rate
       END AS rate_eur,
       session.booking_engine_transaction
       FROM (SELECT * FROM rates_temp) as rates_temp
                        INNER JOIN
                                (SELECT session_id, channel_name, MAX(rn) as rn FROM rates_temp GROUP BY session_id, channel_name) AS inner_q
                                ON rates_temp.session_id = inner_q.session_id
                                                AND rates_temp.channel_name = inner_q.channel_name
                                                AND rates_temp.rn = inner_q.rn
                        INNER JOIN                 
                                (SELECT session.utc_time,session.booking_engine_transaction,session.titan_session_id FROM titan_clean.session) as session
                                 ON rates_temp.session_id = session.titan_session_id
                        LEFT JOIN 
                                (SELECT currency.id,currency.code FROM general.currency) as currency
                                 ON rates_temp.currency_code = currency.code
                        LEFT JOIN
                                (SELECT currency_exchange_rates.currency_id,DATE(currency_exchange_rates.date),currency_exchange_rates.rate FROM  general.currency_exchange_rates) as currency_exchange_rates
                                ON currency.id = currency_exchange_rates.currency_id
                                AND DATE(rates_temp.fetched_at) = DATE(currency_exchange_rates.date)
WHERE rate_eur > 0);                                   
                    

DROP TABLE IF EXISTS rates_stg;
CREATE TABLE rates_stg AS
(SELECT
            rates_temp.*,
            rates_latest_temp.utc_time,
            DATEDIFF('days', rates_temp.arrival_date, rates_temp.departure_date) AS length_of_stay,
            rates_latest_temp.rate_eur,
            rates_direct.rate_eur AS rate_direct_eur,
            rates_direct.rate_eur - rates_latest_temp.rate_eur AS delta_direct,
            100.0*(rates_direct.rate_eur - rates_latest_temp.rate_eur)/rates_direct.rate_eur AS delta_direct_pct,
            rates_latest_temp.booking_engine_transaction,
            CASE
              WHEN (rates_direct.rate_eur - rates_latest_temp.rate_eur)>-1 AND (rates_direct.rate_eur - rates_latest_temp.rate_eur) <1 THEN 0
              ELSE (rates_direct.rate_eur - rates_latest_temp.rate_eur)
            END AS adjusted_delta_direct,
            CASE
              WHEN (100.0*(rates_direct.rate_eur - rates_latest_temp.rate_eur)/rates_direct.rate_eur)>(-100/rates_direct.rate_eur) AND
                   (100.0*(rates_direct.rate_eur - rates_latest_temp.rate_eur)/rates_direct.rate_eur)<(100/rates_direct.rate_eur) THEN 0
              ELSE (100.0*(rates_direct.rate_eur - rates_latest_temp.rate_eur)/rates_direct.rate_eur)
            END AS adjusted_delta_direct_pct
        FROM
            rates_temp as rates_temp
        INNER JOIN
                 (SELECT * FROM rates_latest_temp) as rates_latest_temp
                               ON  rates_temp.fetched_at = rates_latest_temp.fetched_at
                               AND rates_temp.session_id = rates_latest_temp.session_id
                               AND rates_temp.channel_name = rates_latest_temp.channel_name
                               AND rates_temp.rate = rates_latest_temp.rate
        INNER JOIN
                 (SELECT rates_latest_temp.session_id,rates_latest_temp.fetched_at,rates_latest_temp.channel_name,rates_latest_temp.rate_eur FROM rates_latest_temp) AS rates_direct
                    ON  rates_temp.session_id = rates_direct.session_id
                    AND rates_temp.fetched_at = rates_direct.fetched_at
                    AND rates_direct.channel_name = 'Direct'
WHERE
            /* Only keep the sessions for which the price difference between direct and OTA is not more tnan 50% */
            ABS(100.0*(rates_direct.rate_eur - rates_latest_temp.rate_eur)/rates_direct.rate_eur) <= 50                 
)
