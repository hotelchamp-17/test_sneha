------------------cd_titan_base_clean-------------------------------------------------
DROP TABLE if exists titan_clean_session_temp;
CREATE TABLE titan_clean_session_temp 
AS
SELECT DISTINCT session.*,
       ROW_NUMBER() OVER (PARTITION BY session.id,session.updated_field ORDER BY session.session_order,session.id) AS source_order
FROM (SELECT DISTINCT session.id,
             session.titan_session_id,
             session.website_hash,
             session.utc_time,
             session.device,
             session.booking_engine_start,
             session.booking_engine_transaction,
             document_connector_event.updated_field,
             ROW_NUMBER() OVER (PARTITION BY session.id ORDER BY COALESCE(document_connector_event.utc_time,pageview.utc_time) DESC) AS session_order,
             document_connector_event.date_value,
             document_connector_event.float_value
      FROM titan_clean.session AS session
        LEFT JOIN titan_clean.pageview ON session.id = pageview.session_id
        LEFT JOIN titan_clean.document_connector_event ON document_connector_event.pageview_id = pageview.id
      WHERE session.utc_time >= '2019-05-01 00:00:00'
      AND   document_connector_event.updated_field IN ('arrivalDate','departureDate','adultCount','childCount','babyCount')
      AND   (document_connector_event.float_value != 0 OR document_connector_event.date_value IS NOT NULL)
      ) session;


drop table if exists cd_titan_base_clean_stg;
create table cd_titan_base_clean_stg as
select 
session_info.id,
session_info.titan_session_id,
session_info.website_hash,
session_info.utc_time,
session_info.device,
session_info.booking_engine_start,
session_info.booking_engine_transaction,
COALESCE(session_info.babyCount,trxn.baby_count)as baby_count,
COALESCE(session_info.childCount,trxn.child_count) as child_count,
COALESCE(session_info.adultCount,trxn.adult_count) as adult_count,
COALESCE(session_info.arrivalDate,trxn.arrival_date) as arrival_date,
COALESCE(session_info.departureDate,trxn.departure_date) as departure_date,
(trxn.transaction_id) as transaction_id
from
(
SELECT id,
       titan_session_id,
       website_hash,
       utc_time,
       device,
       booking_engine_start,
       booking_engine_transaction,
       MAX(CASE WHEN updated_field = 'babyCount' AND float_value != 0 THEN float_value ELSE 0 END) babyCount,
       MAX(CASE WHEN updated_field = 'childCount' AND float_value != 0 THEN float_value ELSE 0 END) childCount,
       MAX(CASE WHEN updated_field = 'adultCount' AND float_value != 0 THEN float_value ELSE 0 END) adultCount,
       MAX(CASE WHEN updated_field = 'arrivalDate' THEN DATE (date_value) ELSE NULL END) arrivalDate,
       MAX(CASE WHEN updated_field = 'departureDate' THEN DATE (date_value) ELSE NULL END) departureDate
FROM titan_clean_session_temp a
WHERE source_order = 1
GROUP BY id,
         titan_session_id,
         website_hash,
         utc_time,
         device,
         booking_engine_start,
         booking_engine_transaction
) as session_info left outer join
(
  SELECT *
  FROM (SELECT session.id as session_id,
               ROW_NUMBER() OVER (PARTITION BY session.id ORDER BY transaction.utc_time ASC) AS rn,
               transaction.id as transaction_id,
               DATE (transaction.arrival_date) AS arrival_date,
               DATE (transaction.departure_date) AS departure_date,
               transaction.adult_count,
               transaction.child_count,
               transaction.baby_count
        FROM titan_clean.session
          LEFT JOIN titan_clean.transaction ON (session.id = transaction.session_id)
        WHERE session.utc_time >= '2019-05-01 00:00:00')
  WHERE rn = 1
) as trxn
on (session_info.id =trxn.session_id)
