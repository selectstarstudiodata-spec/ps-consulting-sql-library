/* ============================================================================
   ETL: PowerSchool -> Infinite Campus
   Target file: studentStandardScore.csv
============================================================================ */

set heading off
set pagesize 0
set feedback off
set underline off

WITH ps_source AS (

  SELECT
    CAST(st.student_number AS VARCHAR2(20)) AS studentNumber,
    asa.standardid,
    ss.yearid,

    COALESCE(
      ss.actualscoreentered,
      ss.scorelettergrade,
      TO_CHAR(ss.scorenumericgrade),
      TO_CHAR(ss.scorepercent)
    ) AS gradeValue,

    ss.isexempt,
    ss.ismissing,
    ss.islate

  FROM ps.standardscore ss

  JOIN ps.assignmentstandardassoc asa
    ON asa.assignmentstandardassocid = ss.assignmentstandardassocid

  JOIN ps.students st
    ON st.dcid = ss.studentsdcid

  WHERE st.student_number IS NOT NULL
),

validated AS (
  SELECT
    studentNumber,
    standardid,
    yearid,
    gradeValue,
    TRIM(
      CASE WHEN studentNumber IS NULL THEN 'ERR_REQUIRED_studentNumber; ' END ||
      CASE WHEN standardid IS NULL THEN 'ERR_REQUIRED_standardID; ' END ||
      CASE WHEN gradeValue IS NULL THEN 'ERR_REQUIRED_gradeValue; ' END
    ) AS validation_errors
  FROM ps_source
)

SELECT
  '"record_status","studentNumber","standardID","yearID","gradeValue","validation_errors"'
FROM dual

UNION ALL

SELECT
  '"'||
    CASE WHEN validation_errors IS NULL OR validation_errors = '' THEN 'CLEAN' ELSE 'ERROR' END
  ||'",'||
  '"'||studentNumber||'",'||
  '"'||standardid||'",'||
  '"'||NVL(TO_CHAR(yearid),'')||'",'||
  '"'||REPLACE(NVL(gradeValue,''),'"','""')||'",'||
  '"'||REPLACE(NVL(validation_errors,''),'"','""')||'"'
FROM validated;