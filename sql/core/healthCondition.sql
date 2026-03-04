set heading off
set pagesize 0
set feedback off
set underline off
set trimspool on
set linesize 32767
set newpage none

/* ============================================================================
   ETL: PowerSchool > Infinite Campus
   Target file: healthCondition.csv
============================================================================ */

WITH
ps_source AS (
  SELECT
    CAST(s.student_number AS VARCHAR2(15)) AS studentNum_raw,
    CAST(hc.healthconcern AS VARCHAR2(10)) AS code_raw,
    CAST(hc.narrative     AS VARCHAR2(60)) AS description_raw,
    hc.startdate                           AS startDate_raw,
    hc.stopdate                            AS endDate_raw,
    CAST('Active' AS VARCHAR2(15))         AS status_raw,
    SUBSTR(TRIM(COALESCE(hc.whocreated, 'SYS')), 1, 10) AS initials_raw,
    CAST(NULL AS VARCHAR2(50))             AS doctorName_raw,
    CAST(NULL AS VARCHAR2(25))             AS doctorPhone_raw,
    CAST(NULL AS VARCHAR2(255))            AS instructions_raw,
    CAST(NULL AS VARCHAR2(30))             AS userWarning_raw
  FROM ps.healthconcerns hc
  JOIN ps.students s
    ON s.dcid = hc.studentsdcid
),

xform AS (
  SELECT
    SUBSTR(TRIM(studentNum_raw), 1, 15)    AS studentNum,
    SUBSTR(TRIM(code_raw), 1, 10)          AS code,
    SUBSTR(TRIM(description_raw), 1, 60)   AS description,
    startDate_raw                          AS startDate,
    endDate_raw                            AS endDate,
    SUBSTR(TRIM(status_raw), 1, 15)        AS status,
    SUBSTR(TRIM(initials_raw), 1, 10)      AS initials,
    SUBSTR(TRIM(doctorName_raw), 1, 50)    AS doctorName,
    SUBSTR(TRIM(doctorPhone_raw), 1, 25)   AS doctorPhone,
    SUBSTR(TRIM(instructions_raw), 1, 255) AS instructions,
    SUBSTR(TRIM(userWarning_raw), 1, 30)   AS userWarning
  FROM ps_source
),

validated AS (
  SELECT *
  FROM (
    SELECT
      x.*,
      TRIM(
        CASE WHEN studentNum IS NULL OR TRIM(studentNum) = '' THEN 'ERR_REQUIRED_studentNum; ' END ||
        CASE WHEN code IS NULL OR TRIM(code) = '' THEN 'ERR_REQUIRED_code; ' END ||
        CASE WHEN description IS NULL OR TRIM(description) = '' THEN 'ERR_REQUIRED_description; ' END ||
        CASE WHEN startDate IS NULL THEN 'ERR_REQUIRED_startDate; ' END ||
        CASE WHEN status IS NULL OR TRIM(status) = '' THEN 'ERR_REQUIRED_status; ' END ||
        CASE WHEN initials IS NULL OR TRIM(initials) = '' THEN 'ERR_REQUIRED_initials; ' END
      ) AS validation_errors,
      ROW_NUMBER() OVER (
        PARTITION BY studentNum, code, startDate
        ORDER BY endDate NULLS LAST
      ) AS rn
    FROM xform x
  )
  WHERE rn = 1
)

SELECT csv_line
FROM (
  SELECT 0 AS sort_order, 0 AS data_order,
    '"row_type","studentNum","code","description","startDate","endDate","status","initials","doctorName","doctorPhone","userWarning","instructions","validation_errors"' AS csv_line
  FROM dual

  UNION ALL

  SELECT
    1,
    ROW_NUMBER() OVER (ORDER BY studentNum, code, startDate),

    '"'||
    CASE WHEN validation_errors IS NULL OR validation_errors = '' THEN 'CLEAN' ELSE 'ERROR' END||'",'||

    '"'||REPLACE(NVL(studentNum,''),'"','""')||'",'||
    '"'||REPLACE(NVL(code,''),'"','""')||'",'||
    '"'||REPLACE(NVL(description,''),'"','""')||'",'||
    '"'||NVL(TO_CHAR(startDate,'MM/DD/YYYY'),'')||'",'||
    '"'||NVL(TO_CHAR(endDate,'MM/DD/YYYY'),'')||'",'||
    '"'||REPLACE(NVL(status,''),'"','""')||'",'||
    '"'||REPLACE(NVL(initials,''),'"','""')||'",'||
    '"'||REPLACE(NVL(doctorName,''),'"','""')||'",'||
    '"'||REPLACE(NVL(doctorPhone,''),'"','""')||'",'||
    '"'||REPLACE(NVL(userWarning,''),'"','""')||'",'||
    '"'||REPLACE(NVL(instructions,''),'"','""')||'",'||
    '"'||REPLACE(NVL(validation_errors,''),'"','""')||'"'

  FROM validated

  UNION ALL

  SELECT
    2,
    0,
    '"ERROR","","","","","","","","","","","","WARN_NO_ROWS_RETURNED"'
  FROM dual
  WHERE NOT EXISTS (SELECT 1 FROM validated)
)
ORDER BY sort_order, data_order;