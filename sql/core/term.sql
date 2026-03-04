/* ============================================================================
   ETL: PowerSchool > Infinite Campus
   Target file: term.csv
============================================================================ */

WITH
school_key_map AS (
    SELECT id AS school_key, school_number AS schoolNum, name AS schoolName FROM ps.schools
    UNION ALL SELECT dcid AS school_key, school_number AS schoolNum, name AS schoolName FROM ps.schools
    UNION ALL SELECT school_number AS school_key, school_number AS schoolNum, name AS schoolName FROM ps.schools
),

active_years AS (
  SELECT DISTINCT t.yearid
  FROM ps.terms t
  WHERE t.isyearrec = 1
    AND (
         TRUNC(SYSDATE) BETWEEN t.firstday AND t.lastday
         OR
         EXTRACT(MONTH FROM t.firstday) IN (6,7)
         AND t.firstday > TRUNC(SYSDATE)
         OR
         t.firstday = (
             SELECT MIN(t2.firstday)
             FROM ps.terms t2
             WHERE t2.isyearrec = 1
               AND t2.firstday > TRUNC(SYSDATE)
               AND EXTRACT(MONTH FROM t2.firstday) >= 7
         )
    )
),

yearrec_terms AS (
  SELECT
    skm.schoolNum,
    skm.schoolName,
    t.id        AS termid,
    t.firstday  AS startDate_dt,
    t.lastday   AS endDate_dt
  FROM ps.terms t
  JOIN school_key_map skm
    ON skm.school_key = t.schoolid
  WHERE t.isyearrec = 1
    AND t.yearid IN (SELECT yearid FROM active_years)
    AND skm.schoolNum IS NOT NULL
    AND skm.schoolNum <> 999999
),

calendar_base AS (
  SELECT
    schoolNum,
    schoolName,
    startDate_dt,
    endDate_dt,
    CASE
      WHEN EXTRACT(MONTH FROM startDate_dt) IN (6,7)
      THEN SUBSTR(TO_CHAR(EXTRACT(YEAR FROM startDate_dt)),-2) || '-' ||
           SUBSTR(TO_CHAR(EXTRACT(YEAR FROM endDate_dt)),-2) || ' Summer School'
      ELSE SUBSTR(TO_CHAR(EXTRACT(YEAR FROM startDate_dt)),-2) || '-' ||
           SUBSTR(TO_CHAR(EXTRACT(YEAR FROM endDate_dt)),-2) || ' ' ||
           SUBSTR(TRIM(schoolName),1,23)
    END AS calendarName_base
  FROM yearrec_terms
),

calendar_final AS (
  SELECT
    schoolNum,
    SUBSTR(calendarName_base,1,30) AS calendarName,
    startDate_dt,
    endDate_dt
  FROM calendar_base
),

active_terms AS (
    SELECT
      skm.schoolNum AS schoolNum_raw,
      cf.calendarName AS calendarName_raw,
      CAST(t.name AS VARCHAR2(30)) AS termName_raw,
      t.firstday AS startDate_dt,
      t.lastday  AS endDate_dt,
      t.id AS termid
    FROM ps.terms t
    JOIN school_key_map skm ON skm.school_key = t.schoolid
    JOIN calendar_final cf ON cf.schoolNum = skm.schoolNum
    WHERE t.yearid IN (SELECT yearid FROM active_years)
      AND NVL(t.isyearrec,0) = 0
      AND skm.schoolNum IS NOT NULL
),

ps_source AS (
    SELECT *
    FROM (
      SELECT a.*,
             ROW_NUMBER() OVER (
               PARTITION BY a.schoolNum_raw, a.calendarName_raw, a.termName_raw
               ORDER BY a.termid
             ) AS rn
      FROM active_terms a
    )
    WHERE rn = 1
),

xform AS (
    SELECT
      SUBSTR(TRIM(schoolNum_raw),1,7) AS schoolNum,
      SUBSTR(TRIM(calendarName_raw),1,30) AS calendarName,
      SUBSTR(TRIM(termName_raw),1,30) AS termName,
      TO_CHAR(startDate_dt,'MM/DD/YYYY') AS startDate,
      TO_CHAR(endDate_dt,'MM/DD/YYYY')   AS endDate
    FROM ps_source
),

validated AS (
    SELECT
      x.*,
      TRIM(
        CASE WHEN schoolNum IS NULL THEN 'ERR_REQUIRED_schoolNum; ' END ||
        CASE WHEN calendarName IS NULL THEN 'ERR_REQUIRED_calendarName; ' END ||
        CASE WHEN termName IS NULL THEN 'ERR_REQUIRED_termName; ' END ||
        CASE WHEN startDate IS NULL THEN 'ERR_REQUIRED_startDate; ' END ||
        CASE WHEN endDate IS NULL THEN 'ERR_REQUIRED_endDate; ' END
      ) AS validation_errors
    FROM xform x
)

SELECT
  '"row_type","schoolNum","calendarName","termName","startDate","endDate","validation_errors"'
FROM dual

UNION ALL

SELECT
  '"'||CASE WHEN validation_errors IS NULL OR validation_errors='' THEN 'CLEAN' ELSE 'ERROR' END||'",'||
  '"'||schoolNum||'",'||
  '"'||REPLACE(calendarName,'"','""')||'",'||
  '"'||REPLACE(termName,'"','""')||'",'||
  '"'||NVL(startDate,'')||'",'||
  '"'||NVL(endDate,'')||'",'||
  '"'||REPLACE(NVL(validation_errors,''),'"','""')||'"'
FROM validated;
