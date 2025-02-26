with t_1 as (
SELECT DISTINCT
DATE_ADD (s.date,INTERVAL es.sent_date DAY ) as sent_day,
DATE_TRUNC(DATE_ADD (s.date,INTERVAL es.sent_date DAY ), MONTH) as sent_month, es.id_account,
COUNT(es.id_message) OVER (PARTITION BY es.id_account, DATE_TRUNC(DATE_ADD(s.date, INTERVAL es.sent_date DAY), MONTH)) AS cnt_msg,
COUNT(es.id_message) OVER (PARTITION BY  DATE_TRUNC(DATE_ADD(s.date, INTERVAL es.sent_date DAY), MONTH)) AS msg_month,
FROM data-analytics-mate.DA.email_sent es
JOIN data-analytics-mate.DA.account_session acs
ON es.id_account = acs.account_id
JOIN data-analytics-mate.DA.session s
ON s.ga_session_id =acs.ga_session_id  ),


t_2 as (
  SELECT  sent_month, id_account,
cnt_msg, msg_month  ,
MIN (sent_day) OVER(PARTITION BY sent_month) AS first_sent_date,
MAX(sent_day) OVER(PARTITION BY sent_month)as last_sent_date
FROM t_1)


SELECT sent_month, id_account,
cnt_msg/msg_month  * 100 AS sent_msg_percent,
first_sent_date,
last_sent_date
FROM t_2

