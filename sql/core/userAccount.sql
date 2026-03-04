set heading off
set pagesize 0
set feedback off
set underline off
set trimspool on
set linesize 32767
set newpage none

/* ============================================================================
  ETL: PowerSchool > Infinite Campus
  Target file: userAccount.csv
============================================================================ */

WITH

staff_src AS (
  SELECT
    CAST(NVL(TO_CHAR(u.teachernumber), TO_CHAR(u.dcid)) AS VARCHAR2(15)) AS employNum_raw,
    CAST(NULL AS VARCHAR2(15))                                            AS studentNum_raw,
    LOWER(TRIM(COALESCE(u.teacherloginid, u.loginid)))                    AS userName_raw,
    CAST(NULL AS VARCHAR2(40))                                            AS password_raw,
    CAST('STAFF' AS VARCHAR2(10))                                         AS accountType_raw,
    1 AS acct_priority
  FROM ps.users u
  WHERE NVL(UPPER(u.last_name), 'X') NOT IN ('POWERSCHOOL','SYSTEM')
    AND COALESCE(u.teacherloginid, u.loginid) IS NOT NULL
    AND TRIM(COALESCE(u.teacherloginid, u.loginid)) IS NOT NULL
    AND (
          u.allowloginstart IS NULL
          OR u.allowloginstart IN (0,-1)
          OR (DATE '1899-12-30' + u.allowloginstart) <= SYSDATE
        )
    AND (
          u.allowloginend IS NULL
          OR u.allowloginend IN (0,-1)
          OR (DATE '1899-12-30' + u.allowloginend) >= SYSDATE
        )
),

student_src AS (
  SELECT
    CAST(NULL AS VARCHAR2(15))                                            AS employNum_raw,
    CAST(s.student_number AS VARCHAR2(15))                                AS studentNum_raw,
    LOWER(TRIM(COALESCE(s.student_web_id, s.web_id)))                     AS userName_raw,
    CAST(NULL AS VARCHAR2(40))                                            AS password_raw,
    CAST('STUDENT' AS VARCHAR2(10))                                       AS accountType_raw,
    2 AS acct_priority
  FROM ps.students s
  WHERE s.student_number IS NOT NULL
    AND s.student_number <> 0
    AND s.entrydate <= SYSDATE
    AND (s.exitdate IS NULL OR s.exitdate >= SYSDATE)
    AND COALESCE(s.student_web_id, s.web_id) IS NOT NULL
    AND TRIM(COALESCE(s.student_web_id, s.web_id)) IS NOT NULL
),

ps_source AS (
  SELECT * FROM staff_src
  UNION ALL
  SELECT * FROM student_src
),

xform AS (
  SELECT
    SUBSTR(TRIM(employNum_raw), 1, 15)   AS employNum,
    SUBSTR(TRIM(studentNum_raw), 1, 15)  AS studentNum,
    SUBSTR(TRIM(userName_raw), 1, 50)    AS userName,
    SUBSTR(TRIM(password_raw), 1, 40)    AS password,
    accountType_raw                      AS accountType,
    acct_priority
  FROM ps_source
),

validated AS (
  SELECT *
  FROM (
    SELECT
      x.*,
      TRIM(
        CASE WHEN userName IS NULL OR LENGTH(TRIM(userName)) = 0
             THEN 'ERR_REQUIRED_userName; ' END ||
        CASE WHEN employNum IS NOT NULL AND LENGTH(TRIM(employNum)) = 0
             THEN 'ERR_INVALID_employNum_blank; ' END ||
        CASE WHEN studentNum IS NOT NULL AND LENGTH(TRIM(studentNum)) = 0
             THEN 'ERR_INVALID_studentNum_blank; ' END ||
        CASE WHEN employNum IS NOT NULL AND studentNum IS NOT NULL
             THEN 'ERR_BOTH_employNum_and_studentNum_set; ' END
      ) AS validation_errors,
      ROW_NUMBER() OVER (
        PARTITION BY userName
        ORDER BY acct_priority
      ) AS rn
    FROM xform x
  )
  WHERE rn = 1
)

SELECT csv_line
FROM (
  SELECT 0 AS sort_order, 0 AS data_order,
         '"row_type","employNum","studentNum","userName","password","accountType","validation_errors"' AS csv_line
  FROM dual

  UNION ALL

  SELECT
    1,
    ROW_NUMBER() OVER (ORDER BY userName),

    '"'||
    CASE WHEN validation_errors IS NULL OR validation_errors = ''
         THEN 'CLEAN' ELSE 'ERROR' END||'",'||

    '"'||REPLACE(NVL(employNum,''),'"','""')||'",'||
    '"'||REPLACE(NVL(studentNum,''),'"','""')||'",'||
    '"'||REPLACE(NVL(userName,''),'"','""')||'",'||
    '"'||REPLACE(NVL(password,''),'"','""')||'",'||
    '"'||REPLACE(NVL(accountType,''),'"','""')||'",'||
    '"'||REPLACE(NVL(validation_errors,''),'"','""')||'"'

  FROM validated
)
ORDER BY sort_order, data_order;