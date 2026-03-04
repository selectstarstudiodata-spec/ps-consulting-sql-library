/* ============================================================================
  ETL: PowerSchool > Infinite Campus
  Target file: homelessService.csv
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
    h.startdate AS startDate,
    CASE
      WHEN NVL(h.transportation,0)=1
        OR NVL(h.tutor_instrsvcs,0)=1
        OR NVL(h.school_supplies,0)=1
        OR NVL(h.clothing,0)=1
        OR NVL(h.counseling,0)=1
        OR NVL(h.health_referrals,0)=1
        OR NVL(h.records_transfer,0)=1
        OR NVL(h.before_after_schl_pgm,0)=1
        OR NVL(h.emer_assist_attendance,0)=1
        OR NVL(h.parent_education,0)=1
        OR NVL(h.staff_prof_dev,0)=1
        OR NVL(h.other_svcs,0)=1
        OR NVL(h.refer_other_pgmsvc,0)=1
        OR NVL(h.assist_participation,0)=1
        OR NVL(h.coord_schl_agency,0)=1
        OR NVL(h.requestsnoservices,0)=1
      THEN 1 ELSE 0
    END AS hasService
  FROM student_scope ss
  JOIN ps.s_nd_stu_homelessprograms_c h
    ON h.studentsdcid = ss.studentdcid
  WHERE h.startdate IS NOT NULL
),

service_rows AS (
  SELECT
    studentNum,
    startDate
  FROM ps_source
  WHERE hasService = 1
),

xform AS (
  SELECT
    SUBSTR(TRIM(studentNum),1,15) AS studentNum,
    startDate
  FROM service_rows
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
    'ERR_NO_SOURCE_ROWS_FOR_HOMELESSSERVICE; '
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