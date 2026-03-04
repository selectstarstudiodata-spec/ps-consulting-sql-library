WITH
student_scope AS (
  SELECT DISTINCT
    s.id AS studentid,
    s.student_number AS studentNum
  FROM ps.students s
  JOIN ps.cc cc
    ON cc.studentid = s.id
  WHERE s.student_number IS NOT NULL
    AND cc.dateenrolled IS NOT NULL
    AND cc.dateenrolled >= DATE '2024-07-01'
    AND cc.dateenrolled <  DATE '2026-07-01'
),

ps_source AS (
  SELECT
    ss.studentNum,
    CAST(NULL AS DATE) AS screening_date_internal
  FROM student_scope ss
),

xform AS (
  SELECT
    CAST(studentNum AS VARCHAR2(15)) AS studentNum,
    screening_date_internal
  FROM ps_source
),

validated AS (
  SELECT
    x.*,
    TRIM(
      CASE WHEN studentNum IS NULL OR TRIM(studentNum)='' THEN 'ERR_REQUIRED_studentNum; ' END ||
      CASE WHEN screening_date_internal IS NULL THEN 'ERR_REQUIRED_date; ' END
    ) AS validation_errors
  FROM xform x
)

SELECT
  '"row_type","studentNum","date","validation_errors"'
FROM dual

UNION ALL

SELECT
  '"'||
  CASE WHEN validation_errors IS NULL OR validation_errors='' THEN 'CLEAN' ELSE 'ERROR' END||'",'||
  '"'||REPLACE(NVL(studentNum,''),'"','""')||'",'||
  '"'||NVL(TO_CHAR(screening_date_internal,'MM/DD/YYYY'),'')||'",'||
  '"'||REPLACE(NVL(validation_errors,''),'"','""')||'"'
FROM validated

UNION ALL

SELECT
  '"ERROR","","","ERR_NO_SOURCE_ROWS_FOR_CHILDANDTEENSCREENING;"'
FROM dual
WHERE NOT EXISTS (SELECT 1 FROM validated);