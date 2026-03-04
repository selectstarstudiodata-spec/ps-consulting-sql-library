/* ============================================================================ */
/* HEADERS FIX */
set termout on
set heading on
set pagesize 50000
set feedback off
set underline off
set trimspool on
set linesize 32767
/* ============================================================================ */

WITH ps_source AS (
  SELECT
    s.student_number              AS studentNum_raw,
    sg.course_number              AS courseNum_raw,
    sg.course_name                AS courseName_raw,
    2026                          AS endYear_raw,
    sg.grade_level                AS gradeLevel_raw,
    sg.grade                      AS score_raw,
    sg.percent                    AS percent_raw,
    sg.earnedcrhrs                AS creditsEarned_raw,
    sg.potentialcrhrs             AS creditsAttempted_raw,
    CASE
      WHEN sg.excludefromgpa = 1 THEN 0
      ELSE sg.potentialcrhrs
    END                           AS gpaWeight_raw,
    NVL(sg.gpa_addedvalue, 0)     AS weightedGPAValue_raw,
    CAST(NULL AS NUMBER)          AS unweightedGPAValue_raw,
    MOD(sg.termid, 100)           AS actualTerm_raw,
    CAST(sg.dcid AS VARCHAR2(50)) AS additionalKey_raw,
    sg.storecode                  AS storedCode_raw,
    sg.datestored                 AS dateStored_raw,
    sg.comment_value              AS comments_raw,
    'STOREDGRADES'                AS sourceTable_raw,
    'CREDITS_COLUMNS_UNKNOWN_IN_PS.STOREDGRADES' AS reviewReason_raw
  FROM ps.storedgrades sg
  JOIN ps.students s
    ON s.id = sg.studentid
  WHERE sg.datestored >= DATE '2024-07-01'
    AND sg.datestored <  DATE '2026-07-01'
),

xform AS (
  SELECT
    SUBSTR(TRIM(studentNum_raw), 1, 15)       AS studentNum,
    SUBSTR(TRIM(courseNum_raw), 1, 13)        AS courseNum,
    SUBSTR(TRIM(courseName_raw), 1, 60)       AS courseName,
    endYear_raw                               AS endYear,
    SUBSTR(TRIM(gradeLevel_raw), 1, 4)        AS gradeLevel,
    SUBSTR(TRIM(score_raw), 1, 10)            AS score,
    percent_raw                               AS percent,
    creditsEarned_raw                         AS creditsEarned,
    creditsAttempted_raw                      AS creditsAttempted,
    gpaWeight_raw                             AS gpaWeight,
    weightedGPAValue_raw                      AS weightedGPAValue,
    unweightedGPAValue_raw                    AS unweightedGPAValue,
    actualTerm_raw                            AS actualTerm,
    SUBSTR(TRIM(additionalKey_raw), 1, 50)    AS additionalKey,
    SUBSTR(TRIM(storedCode_raw), 1, 10)       AS storedCode,
    dateStored_raw                            AS dateStored,
    SUBSTR(
      TRIM(
        CASE
          WHEN comments_raw IS NULL THEN NULL
          ELSE DBMS_LOB.SUBSTR(TO_CLOB(comments_raw), 1200, 1)
        END
      ),
      1,
      1200
    ) AS comments,
    sourceTable_raw                           AS sourceTable,
    SUBSTR(TRIM(reviewReason_raw), 1, 50)     AS reviewReason
  FROM ps_source
),

validated AS (
  SELECT *
  FROM (
    SELECT
      x.*,
      TRIM(
        CASE WHEN studentNum IS NULL OR TRIM(studentNum) = '' THEN 'ERR_REQUIRED_studentNum; ' END ||
        CASE WHEN courseNum IS NULL OR TRIM(courseNum) = '' THEN 'ERR_REQUIRED_courseNum; ' END ||
        CASE WHEN endYear IS NULL THEN 'ERR_REQUIRED_endYear; ' END ||
        CASE WHEN gradeLevel IS NULL OR TRIM(gradeLevel) = '' THEN 'ERR_REQUIRED_gradeLevel; ' END ||
        CASE WHEN score IS NULL OR TRIM(score) = '' THEN 'ERR_REQUIRED_score; ' END ||
        CASE WHEN creditsEarned IS NULL THEN 'ERR_REQUIRED_creditsEarned; ' END ||
        CASE WHEN creditsAttempted IS NULL THEN 'ERR_REQUIRED_creditsAttempted; ' END ||
        CASE WHEN additionalKey IS NULL OR TRIM(additionalKey) = '' THEN 'ERR_REQUIRED_additionalKey; ' END
      ) AS validation_errors,
      ROW_NUMBER() OVER (
        PARTITION BY studentNum, courseNum, endYear, actualTerm, additionalKey
        ORDER BY additionalKey
      ) AS rn
    FROM xform x
  )
  WHERE rn = 1
),

final_data AS (
  SELECT
    CASE
      WHEN validation_errors IS NOT NULL AND TRIM(validation_errors) <> '' THEN 'ERROR'
      ELSE 'CLEAN'
    END AS row_type,
    studentNum,
    courseNum,
    '"' || REPLACE(REPLACE(REPLACE(courseName, '"', '""'), CHR(13), ' '), CHR(10), ' ') || '"' AS courseName,
    endYear,
    '"' || REPLACE(REPLACE(REPLACE(gradeLevel, '"', '""'), CHR(13), ' '), CHR(10), ' ') || '"' AS gradeLevel,
    '"' || REPLACE(REPLACE(REPLACE(score, '"', '""'), CHR(13), ' '), CHR(10), ' ') || '"' AS score,
    percent,
    creditsEarned,
    creditsAttempted,
    gpaWeight,
    weightedGPAValue,
    unweightedGPAValue,
    actualTerm,
    '"' || REPLACE(REPLACE(REPLACE(additionalKey, '"', '""'), CHR(13), ' '), CHR(10), ' ') || '"' AS additionalKey,
    '"' || REPLACE(REPLACE(REPLACE(storedCode, '"', '""'), CHR(13), ' '), CHR(10), ' ') || '"' AS storedCode,
    NVL(TO_CHAR(dateStored, 'YYYY-MM-DD'), '') AS dateStored,
    '"' || REPLACE(REPLACE(REPLACE(comments, '"', '""'), CHR(13), ' '), CHR(10), ' ') || '"' AS comments,
    '"' || REPLACE(REPLACE(REPLACE(sourceTable, '"', '""'), CHR(13), ' '), CHR(10), ' ') || '"' AS sourceTable,
    '"' || REPLACE(REPLACE(REPLACE(reviewReason, '"', '""'), CHR(13), ' '), CHR(10), ' ') || '"' AS reviewReason,
    '"' || REPLACE(REPLACE(REPLACE(NVL(validation_errors,''), '"', '""'), CHR(13), ' '), CHR(10), ' ') || '"' AS validation_errors
  FROM validated
)

SELECT
  row_type,
  studentNum,
  courseNum,
  courseName,
  endYear,
  gradeLevel,
  score,
  percent,
  creditsEarned,
  creditsAttempted,
  gpaWeight,
  weightedGPAValue,
  unweightedGPAValue,
  actualTerm,
  additionalKey,
  storedCode,
  dateStored,
  comments,
  sourceTable,
  reviewReason,
  validation_errors
FROM final_data
ORDER BY studentNum, courseNum, endYear, actualTerm, additionalKey;