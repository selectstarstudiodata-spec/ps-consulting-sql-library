/* ============================================================================
  ETL: PowerSchool > Infinite Campus
  Target file: courseFee.csv
============================================================================ */

set heading off
set pagesize 0
set feedback off
set underline off

WITH active_year AS (
  SELECT MAX(t.yearid) AS active_yearid
  FROM ps.sections sec
  JOIN ps.terms t ON t.id = sec.termid
),

school_scope AS (
  SELECT DISTINCT
    sch.id,
    sch.school_number
  FROM ps.schools sch
  JOIN ps.sections sec ON sec.schoolid = sch.id
  JOIN ps.terms t ON t.id = sec.termid
  WHERE t.yearid = (SELECT active_yearid FROM active_year)
),

ps_source AS (
  SELECT DISTINCT

    CAST(f.fee_type_name AS VARCHAR2(30)) AS feeName,

    CAST(ss.school_number AS VARCHAR2(7)) AS schoolNum,

    CAST(t.abbreviation AS VARCHAR2(30)) AS calendarName,

    CAST(f.course_number AS VARCHAR2(13)) AS courseNum

  FROM ps.fee f
  JOIN ps.sections sec
    ON sec.course_number = f.course_number
  JOIN ps.terms t
    ON t.id = sec.termid
  JOIN school_scope ss
    ON ss.id = sec.schoolid

  WHERE f.course_number IS NOT NULL
    AND f.yearid = (SELECT active_yearid FROM active_year)
),

validated AS (
  SELECT
    feeName,
    schoolNum,
    calendarName,
    courseNum,
    TRIM(
      CASE WHEN feeName IS NULL OR TRIM(feeName) = '' THEN 'ERR_REQUIRED_feeName; ' END ||
      CASE WHEN schoolNum IS NULL OR TRIM(schoolNum) = '' THEN 'ERR_REQUIRED_schoolNum; ' END ||
      CASE WHEN calendarName IS NULL OR TRIM(calendarName) = '' THEN 'ERR_REQUIRED_calendarName; ' END ||
      CASE WHEN courseNum IS NULL OR TRIM(courseNum) = '' THEN 'ERR_REQUIRED_courseNum; ' END
    ) AS validation_errors
  FROM ps_source
),

final_rows AS (
  SELECT
    2 AS sort_order,
    CASE WHEN validation_errors IS NULL OR validation_errors = '' THEN 'CLEAN' ELSE 'ERROR' END AS row_type,
    feeName,
    schoolNum,
    calendarName,
    courseNum,
    validation_errors
  FROM validated

  UNION ALL

  SELECT
    2,
    'ERROR',
    NULL,
    NULL,
    NULL,
    NULL,
    'ERR_NO_SOURCE_ROWS_FOR_COURSEFEE; '
  FROM dual
  WHERE NOT EXISTS (SELECT 1 FROM ps_source)
)

SELECT csv_line
FROM (
  SELECT
    1 AS sort_order,
    '"row_type","feeName","schoolNum","calendarName","courseNum","validation_errors"' AS csv_line
  FROM dual

  UNION ALL

  SELECT
    sort_order,
    '"'||REPLACE(NVL(row_type,''),'"','""')||'",'|| 
    '"'||REPLACE(NVL(feeName,''),'"','""')||'",'|| 
    '"'||REPLACE(NVL(schoolNum,''),'"','""')||'",'|| 
    '"'||REPLACE(NVL(calendarName,''),'"','""')||'",'|| 
    '"'||REPLACE(NVL(courseNum,''),'"','""')||'",'|| 
    '"'||REPLACE(NVL(validation_errors,''),'"','""')||'"'
  FROM final_rows
)
ORDER BY sort_order;