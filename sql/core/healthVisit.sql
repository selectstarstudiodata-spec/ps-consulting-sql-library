set heading off
set pagesize 0
set feedback off
set underline off

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
    CAST(NULL AS DATE) AS visitDate,
    CAST(NULL AS VARCHAR2(5)) AS visitTime,
    CAST(NULL AS VARCHAR2(15)) AS employNum
  FROM student_scope ss
),

xform AS (
  SELECT
    CAST(studentNum AS VARCHAR2(15)) AS studentNum,
    visitDate,
    visitTime,
    employNum
  FROM ps_source
),

validated AS (
  SELECT
    x.*,
    TRIM(
      CASE WHEN studentNum IS NULL OR TRIM(studentNum)='' THEN 'ERR_REQUIRED_studentNum; ' END ||
      CASE WHEN visitDate IS NULL THEN 'ERR_REQUIRED_visitDate; ' END ||
      CASE WHEN visitTime IS NULL OR TRIM(visitTime)='' THEN 'ERR_REQUIRED_visitTime; ' END ||
      CASE WHEN employNum IS NULL OR TRIM(employNum)='' THEN 'ERR_REQUIRED_employNum; ' END
    ) AS validation_errors
  FROM xform x
)

SELECT
  '"row_type","studentNum","visitDate","visitTime","employNum","validation_errors"'
FROM dual

UNION ALL

SELECT
  '"'||
  CASE WHEN validation_errors IS NULL OR validation_errors='' THEN 'CLEAN' ELSE 'ERROR' END||'",'||
  '"'||REPLACE(NVL(studentNum,''),'"','""')||'",'||
  '"'||NVL(TO_CHAR(visitDate,'MM/DD/YYYY'),'')||'",'||
  '"'||REPLACE(NVL(visitTime,''),'"','""')||'",'||
  '"'||REPLACE(NVL(employNum,''),'"','""')||'",'||
  '"'||REPLACE(NVL(validation_errors,''),'"','""')||'"'
FROM validated

UNION ALL

SELECT
  '"ERROR","","","","","ERR_NO_SOURCE_ROWS_FOR_HEALTHVISIT;"'
FROM dual
WHERE NOT EXISTS (SELECT 1 FROM validated);