/* ============================================================================
   ETL: PowerSchool > Infinite Campus
   Target file: department.csv

   Required Columns:
   - schoolNum
   - departmentName

   Unique Key:
   - schoolNum, departmentName

   Constraint:
   - schoolNum must exist in school.csv

   Logic:
   - Use courses.sched_department when populated.
   - If no departments exist anywhere, emit 'General' per school.
============================================================================ */

WITH
school_key_map AS (
    SELECT id AS school_key, school_number AS schoolNum FROM ps.schools
    UNION ALL SELECT dcid AS school_key, school_number AS schoolNum FROM ps.schools
    UNION ALL SELECT school_number AS school_key, school_number AS schoolNum FROM ps.schools
),

active_year AS (
    SELECT MAX(t.yearid) AS active_yearid
    FROM ps.sections sec
    JOIN ps.terms t ON t.id = sec.termid
),

year_window AS (
    SELECT
      (SELECT active_yearid FROM active_year) AS active_yearid,
      (SELECT active_yearid FROM active_year) - 6 AS min_yearid
    FROM dual
),

year_term AS (
    SELECT
      skm.schoolNum,
      t.id AS year_termid,
      t.name AS calendarName,
      t.firstday AS startDate_dt,
      t.lastday  AS endDate_dt,
      t.yearid
    FROM ps.terms t
    LEFT JOIN school_key_map skm
      ON skm.school_key = t.schoolid
    WHERE t.isyearrec = 1
      AND t.yearid = (SELECT active_yearid FROM active_year)
),

dept_from_courses AS (
    SELECT DISTINCT
      CAST(TO_CHAR(skm.schoolNum) AS VARCHAR2(7)) AS schoolNum_raw,
      CAST(TRIM(c.sched_department) AS VARCHAR2(50)) AS departmentName_raw
    FROM ps.sections sec
    JOIN ps.terms t ON t.id = sec.termid
    JOIN ps.courses c ON c.course_number = sec.course_number
    LEFT JOIN school_key_map skm ON skm.school_key = sec.schoolid
    WHERE t.yearid = (SELECT active_yearid FROM active_year)
      AND c.sched_department IS NOT NULL
      AND TRIM(c.sched_department) <> ''
),

dept_exists AS (
    SELECT COUNT(*) AS cnt FROM dept_from_courses
),

ps_source AS (
    SELECT schoolNum_raw, departmentName_raw
    FROM dept_from_courses

    UNION ALL

    SELECT
      CAST(TO_CHAR(s.school_number) AS VARCHAR2(7)) AS schoolNum_raw,
      CAST('General' AS VARCHAR2(50)) AS departmentName_raw
    FROM ps.schools s
    WHERE (SELECT cnt FROM dept_exists) = 0
      AND s.school_number IS NOT NULL
),

xform AS (
    SELECT
      SUBSTR(TRIM(schoolNum_raw), 1, 7) AS schoolNum,
      SUBSTR(TRIM(departmentName_raw), 1, 50) AS departmentName
    FROM ps_source
),

validated AS (
    SELECT
      x.*,
      TRIM(
        CASE WHEN schoolNum IS NULL OR TRIM(schoolNum) = '' THEN 'ERR_REQUIRED_schoolNum; ' END ||
        CASE WHEN departmentName IS NULL OR TRIM(departmentName) = '' THEN 'ERR_REQUIRED_departmentName; ' END
      ) AS validation_errors
    FROM xform x
)

SELECT
  '"row_type","schoolNum","departmentName","validation_errors"'
FROM dual

UNION ALL

SELECT
  '"'||CASE WHEN validation_errors IS NULL OR validation_errors='' THEN 'CLEAN' ELSE 'ERROR' END||'",'||
  '"'||REPLACE(NVL(schoolNum,''),'"','""')||'",'||
  '"'||REPLACE(NVL(departmentName,''),'"','""')||'",'||
  '"'||REPLACE(NVL(validation_errors,''),'"','""')||'"'
FROM validated;
