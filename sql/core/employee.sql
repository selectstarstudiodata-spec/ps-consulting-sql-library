set heading off
set pagesize 0
set feedback off
set underline off
set trimspool on
set linesize 32767
set newpage none

/* ============================================================================
  ETL: PowerSchool > Infinite Campus
  Target file: employee.csv
============================================================================ */

WITH
ps_source AS (
  SELECT
    CAST(NVL(TO_CHAR(u.teachernumber), TO_CHAR(u.dcid)) AS VARCHAR2(15)) AS employNum_raw,
    CAST(u.last_name AS VARCHAR2(50))                                   AS lastName_raw,
    CAST(u.first_name AS VARCHAR2(50))                                  AS firstName_raw,
    CAST(u.whencreated AS DATE)                                         AS districtStartDate_raw,
    CAST(u.middle_name AS VARCHAR2(50))                                 AS middleName_raw,
    CAST(u.email_addr AS VARCHAR2(150))                                 AS email_raw
  FROM ps.users u
  WHERE NVL(UPPER(u.last_name), 'X') NOT IN ('POWERSCHOOL','SYSTEM')
),

xform AS (
  SELECT
    SUBSTR(TRIM(employNum_raw), 1, 15)   AS employNum,
    SUBSTR(TRIM(lastName_raw), 1, 50)    AS lastName,
    SUBSTR(TRIM(firstName_raw), 1, 50)   AS firstName,
    districtStartDate_raw                AS districtStartDate,
    SUBSTR(TRIM(middleName_raw), 1, 50)  AS middleName,
    SUBSTR(TRIM(email_raw), 1, 150)      AS email
  FROM ps_source
),

validated AS (
  SELECT *
  FROM (
    SELECT
      x.*,
      TRIM(
        CASE WHEN employNum IS NULL OR TRIM(employNum) = '' THEN 'ERR_REQUIRED_employNum; ' END ||
        CASE WHEN lastName IS NULL OR TRIM(lastName) = '' THEN 'ERR_REQUIRED_lastName; ' END ||
        CASE WHEN firstName IS NULL OR TRIM(firstName) = '' THEN 'ERR_REQUIRED_firstName; ' END ||
        CASE WHEN districtStartDate IS NULL THEN 'ERR_REQUIRED_districtStartDate; ' END
      ) AS validation_errors,
      ROW_NUMBER() OVER (
        PARTITION BY employNum
        ORDER BY districtStartDate NULLS LAST
      ) AS rn
    FROM xform x
  )
  WHERE rn = 1
)

SELECT csv_line
FROM (
  SELECT 0 AS sort_order, 0 AS data_order,
         '"row_type","employNum","lastName","firstName","districtStartDate","middleName","email","validation_errors"' AS csv_line
  FROM dual

  UNION ALL

  SELECT
    1,
    ROW_NUMBER() OVER (ORDER BY employNum),

    '"'||
    CASE
      WHEN validation_errors IS NULL OR validation_errors = ''
      THEN 'CLEAN'
      ELSE 'ERROR'
    END||'",'||

    '"'||REPLACE(NVL(employNum,''),'"','""')||'",'||
    '"'||REPLACE(NVL(lastName,''),'"','""')||'",'||
    '"'||REPLACE(NVL(firstName,''),'"','""')||'",'||
    '"'||NVL(TO_CHAR(districtStartDate,'MM/DD/YYYY'),'')||'",'||
    '"'||REPLACE(NVL(middleName,''),'"','""')||'",'||
    '"'||REPLACE(NVL(email,''),'"','""')||'",'||
    '"'||REPLACE(NVL(validation_errors,''),'"','""')||'"'

  FROM validated
)
ORDER BY sort_order, data_order;