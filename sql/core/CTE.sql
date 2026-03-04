/* ============================================================================
  ETL: PowerSchool > Infinite Campus
  Target file: cte.csv
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

student_scope AS (
  SELECT DISTINCT
    s.id AS studentid,
    s.student_number AS studentNum,
    cc.dateenrolled AS startDate,
    cc.course_number
  FROM ps.students s
  JOIN ps.cc cc
    ON cc.studentid = s.id
  JOIN active_yearid ay
    ON 1=1
  WHERE cc.termid IS NOT NULL
    AND FLOOR(cc.termid/100) BETWEEN (ay.yearid - 6) AND ay.yearid
    AND s.student_number IS NOT NULL
),

ps_source AS (
  SELECT DISTINCT
    ss.studentNum,
    NVL(ss.startDate, TRUNC(SYSDATE)) AS startDate
  FROM student_scope ss
  JOIN ps.courses c
    ON c.course_number = ss.course_number
  WHERE
        (c.vocational = 1)
     OR (UPPER(c.course_name) LIKE '%CTE%')
     OR (UPPER(c.course_name) LIKE '%CAREER%')
     OR (UPPER(c.course_name) LIKE '%TECH%')
     OR (UPPER(c.course_name) LIKE '%AG%')
     OR (UPPER(c.course_name) LIKE '%WELD%')
     OR (UPPER(c.course_name) LIKE '%AUTO%')
     OR (UPPER(c.course_name) LIKE '%MED%')
),

validated AS (
  SELECT
    studentNum,
    startDate,
    TRIM(
      CASE WHEN studentNum IS NULL OR TRIM(studentNum) = '' THEN 'ERR_REQUIRED_studentNum; ' END ||
      CASE WHEN startDate IS NULL THEN 'ERR_REQUIRED_startDate; ' END
    ) AS validation_errors
  FROM ps_source
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
    'ERR_NO_CTE_SOURCE_ROWS_FOUND; '
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