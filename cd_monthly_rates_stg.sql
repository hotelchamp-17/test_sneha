------------------cd_monthly_rates--------------------------------------
DROP TABLE if exists cd_monthly_rates_stg;
CREATE TABLE cd_monthly_rates_stg AS
  SELECT
      DATE_TRUNC('month', a.utc_time ) AS month,
      a.website_hash,
      COUNT(DISTINCT a.id) AS transactions_valid,
      SUM(a.total_net_value_eur) AS sum_total_net_value_eur,
      SUM(a.total_net_value_eur)/COUNT(DISTINCT id) AS avg_total_net_value_eur
  FROM
      (
      SELECT
          session.website_hash,
          transaction.id,
          transaction.session_id,
          transaction.utc_time,
          round(transaction.total_net_value_cents / 100 / transaction.exchange_rate,2) AS total_net_value_eur
      FROM
          titan_clean.session
      LEFT JOIN
          titan_clean.transaction
          ON session.id = transaction.session_id
      WHERE
          transaction.total_net_value_cents != 0 AND
          /* Return last month only */
          transaction.utc_time >= DATEADD(day,-30, DATE_TRUNC('day',GETDATE()) )
  )a
  WHERE
      a.utc_time >= DATEADD(month,-1, DATE_TRUNC('month', DATE_TRUNC('day',GETDATE())) )
      AND a.utc_time < DATEADD(month,1, DATEADD(month,-1, DATE_TRUNC('month', DATE_TRUNC('day',GETDATE())) ) )
  GROUP BY
      DATE_TRUNC('month', a.utc_time ),
      a.website_hash 








