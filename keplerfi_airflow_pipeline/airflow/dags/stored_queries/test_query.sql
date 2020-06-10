CREATE TABLE IF NOT EXISTS `will_airflow_db.one_day_diff_partition`
  partition by date
  cluster by ticker as
    SELECT a.date, a.ticker,  a.close/b.close-1 as value
    FROM
        (SELECT ticker, date, close from will_airflow_db.price_data
            WHERE ticker = 'FB'
            AND date = '<DATEID>') a

        LEFT OUTER JOIN

        (SELECT ticker, date, close from will_airflow_db.price_data
            WHERE ticker = 'FB'
            AND date = DATE_SUB('<DATEID>', INTERVAL 1 DAY)) b
        ON a.ticker = b.ticker

