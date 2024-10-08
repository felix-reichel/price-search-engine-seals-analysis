--- 0815-at example query
SELECT produkt_id, haendler_bez, COUNT(*) AS num_records,
       MIN(dtimebegin) AS first_listed, MAX(dtimeend) AS last_listed
FROM read_parquet([
    '/scratch0/zieg/MeJ_Tests.d/angebot_06_10.pq/angebot_20??w*.parquet',
    '/scratch0/zieg/MeJ_Tests.d/angebot_11_14.pq/angebot_20??w*.parquet',
    '/scratch0/zieg/MeJ_Tests.d/angebot_15_19.pq/angebot_20??w*.parquet',
    '/scratch0/zieg/MeJ_Tests.d/angebot_20_24.pq/angebot_20??w*.parquet'
], union_by_name=True)
WHERE haendler_bez = '0815-at'
GROUP BY produkt_id, haendler_bez
HAVING MIN(dtimebegin) < (1184198400 - 26 * 7 * 24 * 3600)
   AND MAX(dtimeend) > (1184198400 + 26 * 7 * 24 * 3600)
   AND COUNT(*) >= 1;

SELECT produkt_id, haendler_bez, COUNT(*) AS num_records,
       MIN(COALESCE(dtimebegin, dtimeend)) AS first_listed,
       MAX(COALESCE(dtimeend, dtimebegin)) AS last_listed
FROM read_parquet([
    '/scratch0/zieg/MeJ_Tests.d/angebot_06_10.pq/angebot_20??w*.parquet',
    '/scratch0/zieg/MeJ_Tests.d/angebot_11_14.pq/angebot_20??w*.parquet',
    '/scratch0/zieg/MeJ_Tests.d/angebot_15_19.pq/angebot_20??w*.parquet',
    '/scratch0/zieg/MeJ_Tests.d/angebot_20_24.pq/angebot_20??w*.parquet'
], union_by_name=True)
WHERE haendler_bez = '0815-at'
GROUP BY produkt_id, haendler_bez
HAVING MIN(COALESCE(dtimebegin, dtimeend)) < (1184198400 - 26 * 7 * 24 * 3600)
   AND MAX(COALESCE(dtimeend, dtimebegin)) > (1184198400 + 26 * 7 * 24 * 3600)
   AND COUNT(*) >= 1;


--- 0815-eu
SELECT produkt_id, haendler_bez, COUNT(*) AS num_records,
       MIN(dtimebegin) AS first_listed, MAX(dtimeend) AS last_listed
FROM read_parquet([
    '/scratch0/zieg/MeJ_Tests.d/angebot_06_10.pq/angebot_20??w*.parquet',
    '/scratch0/zieg/MeJ_Tests.d/angebot_11_14.pq/angebot_20??w*.parquet',
    '/scratch0/zieg/MeJ_Tests.d/angebot_15_19.pq/angebot_20??w*.parquet',
    '/scratch0/zieg/MeJ_Tests.d/angebot_20_24.pq/angebot_20??w*.parquet'
], union_by_name=True)
WHERE haendler_bez = 'eu0815-eu'
GROUP BY produkt_id, haendler_bez
HAVING MIN(dtimebegin) < (1575504000 - 26 * 7 * 24 * 3600)
   AND MAX(dtimeend) > (1575504000 + 26 * 7 * 24 * 3600)
   AND COUNT(*) >= 1;


SELECT produkt_id, haendler_bez, COUNT(*) AS num_records,
       MIN(COALESCE(dtimebegin, dtimeend)) AS first_listed,
       MAX(COALESCE(dtimeend, dtimebegin)) AS last_listed
FROM read_parquet([
    '/scratch0/zieg/MeJ_Tests.d/angebot_06_10.pq/angebot_20??w*.parquet',
    '/scratch0/zieg/MeJ_Tests.d/angebot_11_14.pq/angebot_20??w*.parquet',
    '/scratch0/zieg/MeJ_Tests.d/angebot_15_19.pq/angebot_20??w*.parquet',
    '/scratch0/zieg/MeJ_Tests.d/angebot_20_24.pq/angebot_20??w*.parquet'
], union_by_name=True)
WHERE haendler_bez = 'eu0815-eu'
GROUP BY produkt_id, haendler_bez
HAVING MIN(COALESCE(dtimebegin, dtimeend)) < (1575504000 - 26 * 7 * 24 * 3600)
   AND MAX(COALESCE(dtimeend, dtimebegin)) > (1575504000 + 26 * 7 * 24 * 3600)
   AND COUNT(*) >= 1;



-- 1ashop
SELECT produkt_id, haendler_bez, COUNT(*) AS num_records,
       MIN(dtimebegin) AS first_listed, MAX(dtimeend) AS last_listed
FROM read_parquet([
    '/scratch0/zieg/MeJ_Tests.d/angebot_06_10.pq/angebot_20??w*.parquet',
    '/scratch0/zieg/MeJ_Tests.d/angebot_11_14.pq/angebot_20??w*.parquet',
    '/scratch0/zieg/MeJ_Tests.d/angebot_15_19.pq/angebot_20??w*.parquet',
    '/scratch0/zieg/MeJ_Tests.d/angebot_20_24.pq/angebot_20??w*.parquet'
], union_by_name=True)
WHERE haendler_bez = '1ashop'
GROUP BY produkt_id, haendler_bez
HAVING MIN(dtimebegin) < (1149456000 - 26 * 7 * 24 * 3600)
   AND MAX(dtimeend) > (1149456000 + 26 * 7 * 24 * 3600)
   AND COUNT(*) >= 1;


SELECT produkt_id, haendler_bez, COUNT(*) AS num_records,
       MIN(COALESCE(dtimebegin, dtimeend)) AS first_listed,
       MAX(COALESCE(dtimeend, dtimebegin)) AS last_listed
FROM read_parquet([
    '/scratch0/zieg/MeJ_Tests.d/angebot_06_10.pq/angebot_20??w*.parquet',
    '/scratch0/zieg/MeJ_Tests.d/angebot_11_14.pq/angebot_20??w*.parquet',
    '/scratch0/zieg/MeJ_Tests.d/angebot_15_19.pq/angebot_20??w*.parquet',
    '/scratch0/zieg/MeJ_Tests.d/angebot_20_24.pq/angebot_20??w*.parquet'
], union_by_name=True)
WHERE haendler_bez = '1ashop'
GROUP BY produkt_id, haendler_bez
HAVING MIN(COALESCE(dtimebegin, dtimeend)) < (1149456000 - 26 * 7 * 24 * 3600)
   AND MAX(COALESCE(dtimeend, dtimebegin)) > (1149456000 + 26 * 7 * 24 * 3600)
   AND COUNT(*) >= 1;




