SELECT t1.name, t1.hours, ROUND(t1.highest, 2) AS highest, t2.ts
FROM (SELECT name, SUBSTR(ts, 12, 2) AS hours, MAX(high) AS highest
      FROM "17"
      WHERE high > 0
      GROUP BY 1, 2) t1
      JOIN (SELECT name, ts, SUBSTR(ts, 12, 2) AS hours, high
            FROM "17") t2
            ON t1.name = t2.name
            AND t1.hours = t2.hours
            AND t1.highest = t2.high
            ORDER BY t1.name, t1.hours;