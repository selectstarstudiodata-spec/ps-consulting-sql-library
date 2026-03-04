/* ============================================================================
  ETL: PowerSchool > Infinite Campus
  Target file: freeReducedEligibility.csv
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
    cc.dateenrolled,
    TRIM(s.lunchstatus) AS lunchstatus
  FROM ps.students s
  JOIN ps.cc cc
    ON cc.studentid = s.id
  JOIN active_yearid ay
    ON 1=1
  WHERE cc.termid IS NOT NULL
    AND FLOOR(cc.termid/100) BETWEEN (ay.yearid - 1) AND ay.yearid
    AND s.student_number IS NOT NULL
),

ps_source AS (
  SELECT
    studentNum,
    CASE
      WHEN UPPER(lunchstatus) IN ('1','F','FREE') THEN 'F'
      WHEN UPPER(lunchstatus) IN ('2','R','REDUCED') THEN 'R'
      ELSE NULL
    END AS eligibility,
    dateenrolled AS startDate,
    NULL AS endDate,
    (1990 + (SELECT yearid FROM active_yearid)) AS endYear
  FROM student_scope
  WHERE UPPER(lunchstatus) IN ('1','2','F','R','FREE','REDUCED')
),

xform AS (
  SELECT
    SUBSTR(TRIM(studentNum),1,15) AS studentNum,
    eligibility,
    startDate,
    endDate,
    endYear
  FROM ps_source
),

validated AS (
  SELECT
    studentNum,
    eligibility,
    startDate,
    endDate,
    endYear,
    TRIM(
      CASE WHEN studentNum IS NULL OR TRIM(studentNum) = '' THEN 'ERR_REQUIRED_studentNum; ' END ||
      CASE WHEN eligibility IS NULL THEN 'ERR_REQUIRED_eligibility; ' END ||
      CASE WHEN startDate IS NULL THEN 'ERR_REQUIRED_startDate; ' END ||
      CASE WHEN endYear IS NULL THEN 'ERR_REQUIRED_endYear; ' END
    ) AS validation_errors
  FROM xform
),

final_rows AS (
  SELECT
    CASE WHEN validation_errors IS NULL OR validation_errors = '' THEN 'CLEAN' ELSE 'ERROR' END AS row_type,
    studentNum,
    eligibility,
    startDate,
    endDate,
    endYear,
    validation_errors
  FROM validated

  UNION ALL

  SELECT
    'ERROR',
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    'ERR_NO_SOURCE_ROWS_FOR_FREEREDUCEDELIGIBILITY; '
  FROM dual
  WHERE NOT EXISTS (SELECT 1 FROM validated)
),

csv_output AS (
  SELECT 0 AS sort_order,
         '"row_type","studentNum","eligibility","startDate","endDate","endYear","validation_errors"' AS csv_line
  FROM dual

  UNION ALL

  SELECT 1,
         '"'||REPLACE(NVL(row_type,''),'"','""')||'",'||
         '"'||REPLACE(NVL(studentNum,''),'"','""')||'",'||
         '"'||REPLACE(NVL(eligibility,''),'"','""')||'",'||
         '"'||NVL(TO_CHAR(startDate,'MM/DD/YYYY'),'')||'",'||
         '"'||NVL(TO_CHAR(endDate,'MM/DD/YYYY'),'')||'",'||
         '"'||NVL(TO_CHAR(endYear),'')||'",'||
         '"'||REPLACE(NVL(validation_errors,''),'"','""')||'"'
  FROM final_rows
)

SELECT csv_line
FROM csv_output
ORDER BY sort_order;