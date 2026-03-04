/* ============================================================================
  ETL: PowerSchool > Infinite Campus
  Target file: homeSchool.csv
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
    CAST(s.student_number AS VARCHAR2(15)) AS studentNum
  FROM ps.students s
  JOIN ps.cc cc
    ON cc.studentid = s.id
  JOIN active_yearid ay ON 1=1
  WHERE cc.termid IS NOT NULL
    AND FLOOR(cc.termid/100) BETWEEN (ay.yearid - 6) AND ay.yearid
    AND s.student_number IS NOT NULL
),

ps_source AS (
  SELECT
    ss.studentNum,
    CAST(NULL AS DATE) AS startDate,
    CAST(NULL AS DATE) AS endDate,
    CAST(NULL AS DATE) AS "date",
    CAST(NULL AS DATE) AS visitDate,
    CAST(NULL AS VARCHAR2(5)) AS visitTime,
    CAST(NULL AS VARCHAR2(50)) AS flagName,
    CAST(NULL AS VARCHAR2(14)) AS programStatus,
    CAST(NULL AS VARCHAR2(15)) AS serviceType,
    CAST(NULL AS VARCHAR2(1))  AS eligibility,
    CAST(NULL AS NUMBER(4))    AS endYear,
    CAST(NULL AS VARCHAR2(30)) AS feeName,
    CAST(NULL AS VARCHAR2(4))  AS feeType,
    CAST(NULL AS NUMBER(6,2))  AS amount,
    CAST(NULL AS VARCHAR2(7))  AS schoolNum,
    CAST(NULL AS VARCHAR2(30)) AS calendarName,
    CAST(NULL AS VARCHAR2(13)) AS courseNum,
    CAST(NULL AS VARCHAR2(10)) AS lockerNum,
    CAST(NULL AS VARCHAR2(3))  AS lockerType,
    CAST(NULL AS VARCHAR2(15)) AS employNum
  FROM student_scope ss
  WHERE 1 = 0
),

validated AS (
  SELECT
    studentNum,
    startDate,
    endDate,
    "date",
    visitDate,
    visitTime,
    flagName,
    programStatus,
    serviceType,
    eligibility,
    endYear,
    feeName,
    feeType,
    amount,
    schoolNum,
    calendarName,
    courseNum,
    lockerNum,
    lockerType,
    employNum,
    TRIM(
      CASE WHEN studentNum IS NULL OR TRIM(studentNum) = '' THEN 'ERR_REQUIRED_studentNum; ' END ||
      CASE WHEN startDate IS NULL THEN 'ERR_REQUIRED_startDate; ' END
    ) AS validation_errors
  FROM ps_source
),

final_rows AS (
  SELECT
    CASE WHEN validation_errors IS NULL OR validation_errors = '' THEN 'CLEAN' ELSE 'ERROR' END AS row_type,
    validated.*
  FROM validated

  UNION ALL

  SELECT
    'ERROR',
    NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
    NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
    'ERR_NO_SOURCE_ROWS_FOR_HOMESCHOOL; '
  FROM dual
  WHERE NOT EXISTS (SELECT 1 FROM validated)
)

SELECT csv_line
FROM (
    SELECT 0 AS sort_order,
           '"row_type","studentNum","startDate","endDate","date","visitDate","visitTime","flagName","programStatus","serviceType","eligibility","endYear","feeName","feeType","amount","schoolNum","calendarName","courseNum","lockerNum","lockerType","employNum","validation_errors"' AS csv_line
    FROM dual

    UNION ALL

    SELECT 1,
           '"'||REPLACE(NVL(row_type,''),'"','""')||'",'||
           '"'||REPLACE(NVL(studentNum,''),'"','""')||'",'||
           '"'||NVL(TO_CHAR(startDate,'MM/DD/YYYY'),'')||'",'||
           '"'||NVL(TO_CHAR(endDate,'MM/DD/YYYY'),'')||'",'||
           '"'||NVL(TO_CHAR("date",'MM/DD/YYYY'),'')||'",'||
           '"'||NVL(TO_CHAR(visitDate,'MM/DD/YYYY'),'')||'",'||
           '"'||REPLACE(NVL(visitTime,''),'"','""')||'",'||
           '"'||REPLACE(NVL(flagName,''),'"','""')||'",'||
           '"'||REPLACE(NVL(programStatus,''),'"','""')||'",'||
           '"'||REPLACE(NVL(serviceType,''),'"','""')||'",'||
           '"'||REPLACE(NVL(eligibility,''),'"','""')||'",'||
           '"'||NVL(TO_CHAR(endYear),'')||'",'||
           '"'||REPLACE(NVL(feeName,''),'"','""')||'",'||
           '"'||REPLACE(NVL(feeType,''),'"','""')||'",'||
           '"'||NVL(TO_CHAR(amount,'FM999999990.00'),'')||'",'||
           '"'||REPLACE(NVL(schoolNum,''),'"','""')||'",'||
           '"'||REPLACE(NVL(calendarName,''),'"','""')||'",'||
           '"'||REPLACE(NVL(courseNum,''),'"','""')||'",'||
           '"'||REPLACE(NVL(lockerNum,''),'"','""')||'",'||
           '"'||REPLACE(NVL(lockerType,''),'"','""')||'",'||
           '"'||REPLACE(NVL(employNum,''),'"','""')||'",'||
           '"'||REPLACE(NVL(validation_errors,''),'"','""')||'"'
    FROM final_rows
)
ORDER BY sort_order;