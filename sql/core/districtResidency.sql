/* ============================================================================
  ETL: PowerSchool > Infinite Campus
  Target file: districtResidency.csv
============================================================================ */
set heading off
set pagesize 0
set feedback off
set underline off
set trimspool on
set linesize 32767

WITH params AS (
  SELECT 2027 AS active_end_year FROM dual
),
yr AS (
  SELECT (active_end_year - 1990) AS requested_yearid FROM params
),
active_yearid AS (
  SELECT
    CASE
      WHEN EXISTS (
        SELECT 1
        FROM ps.cc cc
        WHERE cc.termid IS NOT NULL
          AND FLOOR(cc.termid/100) = (SELECT requested_yearid FROM yr)
      )
      THEN (SELECT requested_yearid FROM yr)
      ELSE (SELECT MAX(FLOOR(termid/100)) FROM ps.cc WHERE termid IS NOT NULL)
    END AS yearid
  FROM dual
),

student_enrollments AS (
  SELECT
    s.student_number AS studentNum,
    MIN(cc.dateenrolled) AS startDate
  FROM ps.students s
  JOIN ps.cc cc
    ON cc.studentid = s.id
  JOIN active_yearid ay
    ON 1=1
  WHERE cc.termid IS NOT NULL
    AND FLOOR(cc.termid/100) BETWEEN (ay.yearid - 6) AND ay.yearid
    AND s.student_number IS NOT NULL
  GROUP BY s.student_number
),

ps_source AS (
  SELECT
    studentNum,
    startDate
  FROM student_enrollments
),

xform AS (
  SELECT
    SUBSTR(TRIM(studentNum),1,15) AS studentNum,
    startDate
  FROM ps_source
),

validated AS (
  SELECT
    studentNum,
    startDate,
    TRIM(
      CASE WHEN studentNum IS NULL OR TRIM(studentNum) = '' THEN 'ERR_REQUIRED_studentNum; ' END ||
      CASE WHEN startDate IS NULL THEN 'ERR_REQUIRED_startDate; ' END
    ) AS validation_errors
  FROM xform
),

final_rows AS (
  SELECT
    CASE WHEN validation_errors IS NULL OR validation_errors = '' THEN 'CLEAN' ELSE 'ERROR' END AS row_type,
    studentNum,
    startDate,
    validation_errors
  FROM validated

  UNION ALL

  SELECT
    'ERROR',
    NULL,
    NULL,
    'ERR_NO_DISTRICT_RESIDENCY_ROWS_FOUND; '
  FROM dual
  WHERE NOT EXISTS (SELECT 1 FROM validated)
),

csv_output AS (
  SELECT 0 AS sort_order,
         '"row_type","studentNum","startDate","validation_errors"' AS csv_line
  FROM dual

  UNION ALL

  SELECT 1,
         '"'||REPLACE(NVL(row_type,''),'"','""')||'",'||
         '"'||REPLACE(NVL(studentNum,''),'"','""')||'",'||
         '"'||NVL(TO_CHAR(startDate,'MM/DD/YYYY'),'')||'",'||
         '"'||REPLACE(NVL(validation_errors,''),'"','""')||'"'
  FROM final_rows
)

SELECT csv_line
FROM csv_output
ORDER BY sort_order;