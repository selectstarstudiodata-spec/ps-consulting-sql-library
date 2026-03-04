/* ============================================================================
  ETL: PowerSchool > Infinite Campus
  Target file: homeless.csv

  Required Columns: studentNum, startDate
  Unique Key: studentNum, startDate

  Business Rules:
  - Years-of-data rule: 7.
  - Never fabricate program rows.
  - Always emit at least one row (ERROR) when no source rows are found so district DQ review is never "silent empty".

  Source:
  - PS.S_ND_STU_HOMELESSPROGRAMS_C (STARTDATE/ENDDATE, STUDENTSDCID)
  - PS.STUDENTS (DCID -> STUDENT_NUMBER)

  Oracle SQL
============================================================================ */
set heading off
set pagesize 0
set feedback off
set underline off

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
    s.dcid           AS studentdcid,
    s.student_number AS studentNum
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
  SELECT
    ss.studentNum,
    h.startdate AS startDate
  FROM student_scope ss
  JOIN ps.s_nd_stu_homelessprograms_c h
    ON h.studentsdcid = ss.studentdcid
  WHERE h.startdate IS NOT NULL
),

xform AS (
  SELECT
    SUBSTR(TRIM(studentNum), 1, 15) AS studentNum,
    startDate
  FROM ps_source
),

validated AS (
  SELECT
    x.*,
    TRIM(
      CASE WHEN studentNum IS NULL OR TRIM(studentNum) = '' THEN 'ERR_REQUIRED_studentNum; ' END ||
      CASE WHEN startDate IS NULL THEN 'ERR_REQUIRED_startDate; ' END
    ) AS validation_errors
  FROM xform x
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
    'ERROR' AS row_type,
    NULL AS studentNum,
    NULL AS startDate,
    'ERR_NO_SOURCE_ROWS_FOR_HOMELESS; ' AS validation_errors
  FROM dual
  WHERE NOT EXISTS (SELECT 1 FROM validated)
)

SELECT line
FROM (
    SELECT 0 AS sort_order,
           '"row_type","studentNum","startDate","validation_errors"' AS line
    FROM dual

    UNION ALL

    SELECT 1,
           '"'||REPLACE(NVL(row_type,''),'"','""')||'",'||
           '"'||REPLACE(NVL(studentNum,''),'"','""')||'",'||
           '"'||NVL(TO_CHAR(startDate,'MM/DD/YYYY'),'')||'",'||
           '"'||REPLACE(NVL(validation_errors,''),'"','""')||'"'
    FROM final_rows
)
ORDER BY sort_order;