CREATE TABLE user_behavior (
    user_id BIGINT,
    item_id BIGINT,
    category_id BIGINT,
    behavior STRING,
    ts TIMESTAMP(3),
    proctime as PROCTIME(),
    WATERMARK FOR ts as ts - INTERVAL '5' SECOND
) WITH (
    'connector.type' = 'kafka',
    'connector.version' = 'universal',
    'connector.topic' = 'user_behavior',
    'connector.startup-mode' = 'earliest-offset',
    'connector.properties.zookeeper.connect' = '172.16.122.24:2181',
    'connector.properties.bootstrap.servers' = '172.16.122.17:9092',
    'format.type' = 'json'
);
CREATE TABLE buy_cnt_per_hour (
    hour_of_day BIGINT,
    buy_cnt BIGINT
) WITH (
    'connector.type' = 'elasticsearch',
    'connector.version' = '7',
    'connector.hosts' = 'http://172.16.122.13:9200',
    'connector.index' = 'buy_cnt_per_hour',
    'connector.document-type' = 'user_behavior',
    'connector.bulk-flush.max-actions' = '1',
    'update-mode' = 'append',
    'format.type' = 'json'
);

INSERT INTO buy_cnt_per_hour
SELECT HOUR(TUMBLE_START(ts, INTERVAL '1' HOUR)), COUNT(*)
FROM user_behavior
WHERE behavior = 'buy'
GROUP BY TUMBLE(ts, INTERVAL '1' HOUR);

--存放每10分钟累计独立用户数
CREATE TABLE cumulative_uv (
    time_str STRING,
    uv BIGINT
) WITH (
    'connector.type' = 'elasticsearch',
    'connector.version' = '7',
    'connector.hosts' = 'http://172.16.122.13:9200',
    'connector.index' = 'cumulative_uv',
    'connector.document-type' = 'user_behavior',
    'update-mode' = 'upsert',
    'format.type' = 'json'
);
CREATE VIEW uv_per_10min AS
SELECT
  MAX(SUBSTR(DATE_FORMAT(ts, 'HH:mm'),1,4) || '0') OVER w AS time_str,
  COUNT(DISTINCT user_id) OVER w AS uv
FROM user_behavior
WINDOW w AS (ORDER BY proctime ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW);
INSERT INTO cumulative_uv
SELECT time_str, MAX(uv)
FROM uv_per_10min
GROUP BY time_str;

-- 创建商品类目维表
CREATE TABLE category_dim (
    sub_category_id BIGINT,
    parent_category_name STRING
) WITH (
    'connector.type' = 'jdbc',
    'connector.url' = 'jdbc:mysql://172.16.122.25:3306/flink',
    'connector.table' = 'category',
    'connector.driver' = 'com.mysql.jdbc.Driver',
    'connector.username' = 'root',
    'connector.password' = 'root',
    'connector.lookup.cache.max-rows' = '5000',
    'connector.lookup.cache.ttl' = '10min'
);

-- 存放商品类目排行表
CREATE TABLE top_category  (
    category_name  STRING,
    buy_cnt  BIGINT
) WITH (
    'connector.type' = 'elasticsearch',
    'connector.version' = '7',
    'connector.hosts' = 'http://172.16.122.13:9200',
    'connector.index' = 'top_category',
    'connector.document-type' = 'user_behavior',
    'update-mode' = 'upsert',
    'format.type' = 'json'
);