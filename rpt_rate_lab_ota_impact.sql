DROP TABLE if exists rpt_rate_lab_ota_impact;
CREATE TABLE rpt_rate_lab_ota_impact AS
SELECT
cd_titan_base_clean.website_hash as "cd_titan_base_clean.website_hash",
cd_titan_base_clean.utc_time as "cd_titan_base_clean.utc_time",
cd_titan_base_clean.device as "cd_titan_base_clean.device",
cd_titan_base_clean.booking_engine_start as "cd_titan_base_clean.booking_engine_start",
cd_titan_base_clean.booking_engine_transaction as "cd_titan_base_clean.booking_engine_transaction",
cd_titan_base_clean.baby_count as  "cd_titan_base_clean.baby_count",
cd_titan_base_clean.child_count as  "cd_titan_base_clean.child_count",
cd_titan_base_clean.adult_count as  "cd_titan_base_clean.adult_count",
cd_titan_base_clean.arrival_date as "cd_titan_base_clean.arrival_date",
cd_titan_base_clean.departure_date as "cd_titan_base_clean.departure_date",
rates_long.utc_time as "rates_long.utc_time",
rates_long.website_hash as "rates_long.website_hash",
rates_long.channel_name as "rates_long.channel_name",
rates_long.rate as "rates_long.rate",
rates_long.currency_code as "rates_long.currency_code",
rates_long.fetched_at as "rates_long.fetched_at",
rates_long.cached as "rates_long.cached",
rates_long.length_of_stay as "rates_long.length_of_stay",
rates_long.rate_eur as "rates_long.rate_eur",
rates_long.rate_direct_eur as "rates_long.rate_direct_eur",
rates_long.delta_direct as "rates_long.delta_direct",
rates_long.delta_direct_pct as "rates_long.delta_direct_pct",
rates_long.booking_engine_transaction as "rates_long.booking_engine_transaction",
rates_long.adjusted_delta_direct as "rates_long.adjusted_delta_direct",
rates_long.adjusted_delta_direct_pct as "rates_long.adjusted_delta_direct_pct",
cd_monthly_rates.avg_total_net_value_eur,
DATE_TRUNC('month',cd_titan_base_clean.utc_time ) AS month,
DATE(MIN((DATE(rates_long.fetched_at ))) ) AS "rates_long.date_min",
DATE(MAX((DATE(rates_long.fetched_at ))) ) AS "rates_long.date_max",
COUNT(DISTINCT rates_long.session_id ) AS "rates_long.session_count",
COUNT(DISTINCT (rates_long.session_id||'_'||rates_long.channel_name) ) AS "rates_long.channel_session_count",
COUNT(DISTINCT CASE WHEN rates_long.booking_engine_transaction  THEN rates_long.session_id ELSE NULL END) AS "rates_long.sessions_with_transactions"
FROM rates_stg AS rates_long 
LEFT JOIN cd_titan_base_clean_stg as cd_titan_base_clean
ON cd_titan_base_clean.titan_session_id = rates_long.session_id	 
LEFT JOIN cd_monthly_rates_stg AS cd_monthly_rates 
ON cd_monthly_rates.website_hash = cd_titan_base_clean.website_hash	 
group by 
cd_titan_base_clean.website_hash,
cd_titan_base_clean.utc_time,
cd_titan_base_clean.device,
cd_titan_base_clean.booking_engine_start,
cd_titan_base_clean.booking_engine_transaction,
cd_titan_base_clean.baby_count,
cd_titan_base_clean.child_count,
cd_titan_base_clean.adult_count,
cd_titan_base_clean.arrival_date,
cd_titan_base_clean.departure_date,
rates_long.utc_time,
rates_long.website_hash,
rates_long.channel_name,
rates_long.rate,
rates_long.currency_code,
rates_long.fetched_at,
rates_long.cached,
rates_long.length_of_stay,
rates_long.rate_eur,
rates_long.rate_direct_eur,
rates_long.delta_direct,
rates_long.delta_direct_pct,
rates_long.booking_engine_transaction,
rates_long.adjusted_delta_direct,
rates_long.adjusted_delta_direct_pct,
cd_monthly_rates.avg_total_net_value_eur;
grant select on rpt_rate_lab_ota_impact to looker,dashboard,sneha,avantika;

