--- Persistent Listing Criterion (Selection Criterion Counter Check)

    -- '0815-at'
/*
[Xenial @ mach]:~> duckdb-1.1.1
v1.1.1 af39bd0dcf
Enter ".help" for usage hints.
Connected to a transient in-memory database.
Use ".open FILENAME" to reopen on a persistent database.
D SET THREADS TO 64;
D
*/
SELECT produkt_id, haendler_bez, COUNT(*) AS num_records,
         COALESCE(MIN(dtimebegin), 0) AS first_listed,
         COALESCE(MAX(dtimeend), 2199999999) AS last_listed
  FROM read_parquet(['/scratch0/zieg/MeJ_Tests.d/angebot_*_*.pq/angebot_20*w*.parquet'])
  WHERE haendler_bez = '0815-at'
  GROUP BY produkt_id, haendler_bez
  HAVING COALESCE(MIN(dtimebegin), 2199999999) < (1184198400 - 26 * 7 * 24 * 3600)
     AND COALESCE(MAX(dtimeend), 0) > (1184198400 + 26 * 7 * 24 * 3600)
     AND COUNT(*) >= 1;

/*

100% ▕████████████████████████████████████████████████████████████▏
┌────────────┬──────────────┬─────────────┬──────────────┬─────────────┐
│ produkt_id │ haendler_bez │ num_records │ first_listed │ last_listed │
│   int32    │   varchar    │    int64    │    int32     │    int64    │
├────────────┴──────────────┴─────────────┴──────────────┴─────────────┤
│                                0 rows                                │
└──────────────────────────────────────────────────────────────────────┘
D



    -- 'eu0815-eu'

D SET THREADS TO 256;
D
*/


  SELECT produkt_id, haendler_bez, COUNT(*) AS num_records,
         COALESCE(MIN(dtimebegin), 0) AS first_listed,
         COALESCE(MAX(dtimeend), 2147483647) AS last_listed
  FROM read_parquet(['/scratch0/zieg/MeJ_Tests.d/angebot_*_*.pq/angebot_20*w*.parquet'])
  WHERE haendler_bez = 'eu0815-eu'
  GROUP BY produkt_id, haendler_bez
  HAVING COALESCE(MIN(dtimebegin), 2199999999) < (1575504000 - 26 * 7 * 24 * 3600)
     AND COALESCE(MAX(dtimeend), 0) > (1575504000 + 26 * 7 * 24 * 3600)
     AND COUNT(*) >= 1;

/*
100% ▕████████████████████████████████████████████████████████████▏
┌────────────┬──────────────┬─────────────┬──────────────┬─────────────┐
│ produkt_id │ haendler_bez │ num_records │ first_listed │ last_listed │
│   int32    │   varchar    │    int64    │    int32     │    int32    │
├────────────┼──────────────┼─────────────┼──────────────┼─────────────┤
│       4105 │ eu0815-eu    │          93 │   1242656349 │  1713597724 │
│     664146 │ eu0815-eu    │         362 │   1380656303 │  1715725327 │
│    1124103 │ eu0815-eu    │         237 │   1401817363 │  1673965329 │
│     997780 │ eu0815-eu    │         830 │   1378407081 │  1716286927 │
│     953109 │ eu0815-eu    │         813 │   1373025821 │  1716286927 │
│     942423 │ eu0815-eu    │          41 │   1383745842 │  1651294092 │
│     540550 │ eu0815-eu    │        1552 │   1363590942 │  1642419904 │
│    1125876 │ eu0815-eu    │          39 │   1401982253 │  1715876527 │
│     930312 │ eu0815-eu    │         392 │   1378830570 │  1698934928 │
│     195475 │ eu0815-eu    │        1979 │   1297704005 │  1693488127 │
│    1054303 │ eu0815-eu    │         580 │   1389606774 │  1715671327 │
│     358695 │ eu0815-eu    │        1940 │   1331823585 │  1708082522 │
│     847928 │ eu0815-eu    │        1413 │   1363641869 │  1715617327 │
│     997765 │ eu0815-eu    │         424 │   1378407081 │  1715595727 │
│     376748 │ eu0815-eu    │         336 │   1373025821 │  1716276127 │
│     833756 │ eu0815-eu    │         608 │   1383076538 │  1665155052 │
│    1070833 │ eu0815-eu    │         318 │   1402396548 │  1669253412 │
│     835634 │ eu0815-eu    │        1506 │   1363590942 │  1626436756 │
│     993093 │ eu0815-eu    │          75 │   1395937117 │  1692710527 │
│     681368 │ eu0815-eu    │         647 │   1322751129 │  1707812532 │
│        ·   │     ·        │           · │        ·     │       ·     │
│        ·   │     ·        │           · │        ·     │       ·     │
│        ·   │     ·        │           · │        ·     │       ·     │
│    1256633 │ eu0815-eu    │          26 │   1519910033 │  1601390115 │
│    1924397 │ eu0815-eu    │          20 │   1545386259 │  1601390115 │
│    1589652 │ eu0815-eu    │          36 │   1519910033 │  1715941327 │
│    1634905 │ eu0815-eu    │          31 │   1519910033 │  1715595727 │
│    1715071 │ eu0815-eu    │          32 │   1519910033 │  1704756132 │
│    1918447 │ eu0815-eu    │          72 │   1545386259 │  1714742527 │
│    1011643 │ eu0815-eu    │          50 │   1542974874 │  1716038527 │
│    1707019 │ eu0815-eu    │          34 │   1510597628 │  1713846127 │
│    1411466 │ eu0815-eu    │          80 │   1460121338 │  1716222127 │
│    1071755 │ eu0815-eu    │          15 │   1519910033 │  1626178667 │
│    1722313 │ eu0815-eu    │          15 │   1519910033 │  1609336307 │
│    1768409 │ eu0815-eu    │          26 │   1518171538 │  1698304928 │
│     971872 │ eu0815-eu    │          11 │   1519910033 │  1601390115 │
│    1228723 │ eu0815-eu    │          15 │   1519910033 │  1625217836 │
│    1586678 │ eu0815-eu    │          10 │   1519910033 │  1601390115 │
│    1709935 │ eu0815-eu    │          26 │   1519718262 │  1715854926 │
│    1370748 │ eu0815-eu    │          24 │   1500906189 │  1610379330 │
│    1918315 │ eu0815-eu    │          10 │   1545386259 │  1594128347 │
│    1119115 │ eu0815-eu    │          18 │   1435880120 │  1716124927 │
│    1939552 │ eu0815-eu    │           8 │   1543860714 │  1607807649 │
├────────────┴──────────────┴─────────────┴──────────────┴─────────────┤
│ 6017 rows (40 shown)                                       5 columns │
└──────────────────────────────────────────────────────────────────────┘
D

 */

     -- '1ashop'

SELECT produkt_id, haendler_bez, COUNT(*) AS num_records,
         COALESCE(MIN(dtimebegin), 0) AS first_listed,
         COALESCE(MAX(dtimeend), 2199999999) AS last_listed
  FROM read_parquet(['/scratch0/zieg/MeJ_Tests.d/angebot_*_*.pq/angebot_20*w*.parquet'])
  WHERE haendler_bez = '1ashop'
  GROUP BY produkt_id, haendler_bez
  HAVING COALESCE(MIN(dtimebegin), 2199999999) < (1149456000 - 26 * 7 * 24 * 3600)
     AND COALESCE(MAX(dtimeend), 0) > (1149456000 + 26 * 7 * 24 * 3600)
     AND COUNT(*) >= 1;

/*
100% ▕████████████████████████████████████████████████████████████▏
┌────────────┬──────────────┬─────────────┬──────────────┬─────────────┐
│ produkt_id │ haendler_bez │ num_records │ first_listed │ last_listed │
│   int32    │   varchar    │    int64    │    int32     │    int64    │
├────────────┴──────────────┴─────────────┴──────────────┴─────────────┤
│                                0 rows                                │
└──────────────────────────────────────────────────────────────────────┘


     -- 'eu0815-eu' (cont.)

    -- WITHOUT NULL HANDLING AND TRUE T_INFLOW [-56;+26] window.


 */
SELECT produkt_id, haendler_bez, COUNT(*) AS num_records,
         MIN(dtimebegin) AS first_listed,
         MAX(dtimeend) AS last_listed
  FROM read_parquet(['/scratch0/zieg/MeJ_Tests.d/angebot_*_*.pq/angebot_20*w*.parquet'])
  WHERE haendler_bez = 'eu0815-eu'
  GROUP BY produkt_id, haendler_bez
  HAVING
      MIN(dtimebegin) < (1575504000 - 52 * 7 * 24 * 3600)
      AND MAX(dtimeend) > (1575504000 + 26 * 7 * 24 * 3600)
      AND COUNT(*) >= 1;

/*

100% ▕████████████████████████████████████████████████████████████▏
┌────────────┬──────────────┬─────────────┬──────────────┬─────────────┐
│ produkt_id │ haendler_bez │ num_records │ first_listed │ last_listed │
│   int32    │   varchar    │    int64    │    int32     │    int32    │
├────────────┼──────────────┼─────────────┼──────────────┼─────────────┤
│     135919 │ eu0815-eu    │         656 │   1373372146 │  1677788532 │
│     109905 │ eu0815-eu    │          58 │   1373278455 │  1707294132 │
│     567998 │ eu0815-eu    │         281 │   1294838313 │  1704723725 │
│      44477 │ eu0815-eu    │         447 │   1348046027 │  1596199957 │
│     410939 │ eu0815-eu    │        1304 │   1356025583 │  1637576627 │
│     510591 │ eu0815-eu    │         309 │   1366715064 │  1716222127 │
│     937241 │ eu0815-eu    │        1296 │   1367953616 │  1603567017 │
│     880183 │ eu0815-eu    │        2365 │   1355848578 │  1716286927 │
│     331697 │ eu0815-eu    │         437 │   1373025821 │  1592764212 │
│     880293 │ eu0815-eu    │         209 │   1364987909 │  1612518467 │
│    1195336 │ eu0815-eu    │         366 │   1458760086 │  1683541332 │
│    1271304 │ eu0815-eu    │         239 │   1434567453 │  1596554266 │
│     185129 │ eu0815-eu    │         889 │   1407519154 │  1715800927 │
│    1230734 │ eu0815-eu    │         661 │   1423850910 │  1704669732 │
│    1442603 │ eu0815-eu    │         224 │   1463057602 │  1616669676 │
│    1425555 │ eu0815-eu    │        1101 │   1460466770 │  1698412928 │
│    1405195 │ eu0815-eu    │        1225 │   1457383488 │  1668540155 │
│    1009966 │ eu0815-eu    │        2167 │   1380720038 │  1710415322 │
│     568046 │ eu0815-eu    │        1199 │   1312991624 │  1656703854 │
│    1183235 │ eu0815-eu    │         565 │   1431688600 │  1600335675 │
│       ·    │     ·        │           · │        ·     │       ·     │
│       ·    │     ·        │           · │        ·     │       ·     │
│       ·    │     ·        │           · │        ·     │       ·     │
│    1535736 │ eu0815-eu    │          33 │   1522849636 │  1609762754 │
│    1588586 │ eu0815-eu    │          10 │   1519910033 │  1601390115 │
│    1590254 │ eu0815-eu    │          10 │   1519910033 │  1601390115 │
│    1717025 │ eu0815-eu    │          11 │   1519910033 │  1601390115 │
│    1722278 │ eu0815-eu    │          15 │   1519910033 │  1612364289 │
│    1722325 │ eu0815-eu    │          14 │   1519910033 │  1604399147 │
│    1722329 │ eu0815-eu    │          16 │   1519910033 │  1601390115 │
│    1722661 │ eu0815-eu    │          22 │   1519910033 │  1603186765 │
│    1734055 │ eu0815-eu    │           9 │   1519910033 │  1601390115 │
│    1780528 │ eu0815-eu    │          10 │   1522849636 │  1605712226 │
│     685457 │ eu0815-eu    │          20 │   1519910033 │  1641229774 │
│    1004844 │ eu0815-eu    │          12 │   1519910033 │  1601390115 │
│     946453 │ eu0815-eu    │          14 │   1542974874 │  1605712226 │
│     898056 │ eu0815-eu    │          25 │   1383579321 │  1703092929 │
│     594562 │ eu0815-eu    │         102 │   1331813692 │  1713975727 │
│    1896016 │ eu0815-eu    │          19 │   1539809402 │  1638146798 │
│     788967 │ eu0815-eu    │          37 │   1431073924 │  1715595727 │
│     808442 │ eu0815-eu    │          40 │   1432116586 │  1715595727 │
│    1464615 │ eu0815-eu    │           8 │   1537201934 │  1649416115 │
│    1075332 │ eu0815-eu    │          40 │   1488573555 │  1715865727 │
├────────────┴──────────────┴─────────────┴──────────────┴─────────────┤
│ 4924 rows (40 shown)                                       5 columns │
└──────────────────────────────────────────────────────────────────────┘
D

 */


     -- 'eu0815-eu' (cont.)

    -- WHAT ABOUT SINGLE TIME SPELLS? ( AND TRUE T_INFLOW [-56;+26] window. )
SELECT produkt_id, haendler_bez, dtimebegin, dtimeend
FROM read_parquet(['/scratch0/zieg/MeJ_Tests.d/angebot_*_*.pq/angebot_20*w*.parquet'])
    WHERE haendler_bez = 'eu0815-eu'
    AND dtimebegin < (1575504000 - 52 * 7 * 24 * 3600)  -- Before inflow period starts
    AND dtimeend > (1575504000 + 26 * 7 * 24 * 3600);   -- After inflow period ends

/*

100% ▕████████████████████████████████████████████████████████████▏
┌────────────┬──────────────┬────────────┬────────────┐
│ produkt_id │ haendler_bez │ dtimebegin │  dtimeend  │
│   int32    │   varchar    │   int32    │   int32    │
├────────────┼──────────────┼────────────┼────────────┤
│    1319124 │ eu0815-eu    │ 1513594789 │ 1591720703 │
│    1507605 │ eu0815-eu    │ 1513594789 │ 1591720703 │
│    1534067 │ eu0815-eu    │ 1513594789 │ 1591720703 │
│    1595668 │ eu0815-eu    │ 1513594789 │ 1591720703 │
│    1802191 │ eu0815-eu    │ 1523441550 │ 1591720703 │
│    1563431 │ eu0815-eu    │ 1525575319 │ 1591720703 │
│    1726747 │ eu0815-eu    │ 1525341830 │ 1591720703 │
│    1856393 │ eu0815-eu    │ 1532521254 │ 1591720703 │
│    1856415 │ eu0815-eu    │ 1532521254 │ 1591720703 │
│    1462837 │ eu0815-eu    │ 1537215520 │ 1591720703 │
│    1464609 │ eu0815-eu    │ 1537378684 │ 1591720703 │
│    1464615 │ eu0815-eu    │ 1537215520 │ 1591720703 │
│    1935069 │ eu0815-eu    │ 1543255265 │ 1591720703 │
├────────────┴──────────────┴────────────┴────────────┤
│ 13 rows                                   4 columns │
└─────────────────────────────────────────────────────┘
 */

    -- '0815-at' (cont.)

SELECT produkt_id, haendler_bez, dtimebegin, dtimeend
FROM read_parquet(['/scratch0/zieg/MeJ_Tests.d/angebot_*_*.pq/angebot_20*w*.parquet'])
WHERE haendler_bez = '0815-at'
  AND dtimebegin < (1184198400 - 52 * 7 * 24 * 3600)  -- Before inflow period starts
  AND dtimeend > (1184198400 + 26 * 7 * 24 * 3600);   -- After inflow period ends


SELECT COUNT(*), produkt_id
FROM read_parquet(['/scratch0/zieg/MeJ_Tests.d/angebot_*_*.pq/angebot_20*w*.parquet'])
WHERE haendler_bez = 'moebel-plus-de'
GROUP BY produkt_id
-- yields full retailer offer history count by prod_id descriptive stats.


--- Cont. - Q. Is the possible bias in our offer inflow strategy sample uniformly distributed???
