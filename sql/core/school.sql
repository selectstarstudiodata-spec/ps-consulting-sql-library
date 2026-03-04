/* ============================================================================
   ETL: PowerSchool > Infinite Campus
   Target file: school.csv
============================================================================ */

WITH
cfg AS (
  SELECT
    0 AS include_synthetic_district_school,
    '0000000' AS synthetic_schoolNum,
    'District Staff / Non-Enrolled' AS synthetic_name,
    '999-999-9999' AS synthetic_phone,
    'Course Catalog' AS synthetic_courseCatalog
  FROM dual
),

ps_source AS (
  SELECT
    CAST(TO_CHAR(s.school_number) AS VARCHAR2(10)) AS schoolNum_raw,
    CAST(TO_CHAR(s.alternate_school_number) AS VARCHAR2(10)) AS stateSchoolNum_raw,
    CAST(s.name AS VARCHAR2(100))                 AS name_raw,
    CAST(TRIM(s.schoolphone) AS VARCHAR2(25))     AS phone_raw,
    CAST(NULL AS VARCHAR2(100))                   AS address_raw,
    CAST(s.schoolcity AS VARCHAR2(50))            AS city_raw,
    CAST(s.schoolstate AS VARCHAR2(2))            AS state_raw,
    CAST(s.schoolzip AS VARCHAR2(10))             AS zip_raw,
    CAST(TRIM(s.schoolfax) AS VARCHAR2(25))       AS fax_raw,
    CAST(TRIM(s.principal) AS VARCHAR2(100))      AS principal_raw,
    CAST(TRIM(s.principalemail) AS VARCHAR2(100)) AS principal_email_raw,
    CAST('Course Catalog' AS VARCHAR2(50))        AS courseCatalog_raw
  FROM ps.schools s
  WHERE s.school_number IS NOT NULL
    AND s.school_number <> 999999
),

ps_source_dedup AS (
  SELECT *
  FROM (
    SELECT
      p.*,
      ROW_NUMBER() OVER (
        PARTITION BY p.schoolNum_raw
        ORDER BY p.name_raw
      ) rn
    FROM ps_source p
  )
  WHERE rn = 1
),

xform AS (
  SELECT
    SUBSTR(TRIM(schoolNum_raw),1,10)       AS schoolNum,
    SUBSTR(TRIM(stateSchoolNum_raw),1,10)  AS stateSchoolNum,
    SUBSTR(TRIM(name_raw),1,100)           AS name,
    CASE
      WHEN phone_raw IS NULL OR TRIM(phone_raw) = '' THEN '999-999-9999'
      ELSE SUBSTR(TRIM(phone_raw),1,25)
    END AS phone,
    SUBSTR(TRIM(address_raw),1,100)        AS address,
    SUBSTR(TRIM(city_raw),1,50)            AS city,
    SUBSTR(TRIM(state_raw),1,2)            AS state,
    SUBSTR(TRIM(zip_raw),1,10)             AS zip,
    SUBSTR(TRIM(fax_raw),1,25)             AS fax,
    SUBSTR(TRIM(principal_raw),1,100)      AS principal,
    SUBSTR(TRIM(principal_email_raw),1,100) AS principal_email,
    SUBSTR(TRIM(courseCatalog_raw),1,50)   AS courseCatalog
  FROM ps_source_dedup
),

validated AS (
  SELECT
    x.*,
    TRIM(
      CASE WHEN schoolNum IS NULL OR TRIM(schoolNum) = '' THEN 'ERR_REQUIRED_schoolNum; ' END ||
      CASE WHEN name IS NULL OR TRIM(name) = '' THEN 'ERR_REQUIRED_name; ' END
    ) AS validation_errors
  FROM xform x
),

final_data AS (
  SELECT
    CASE
      WHEN validation_errors IS NULL OR TRIM(validation_errors) = '' THEN 'CLEAN'
      ELSE 'ERROR'
    END AS row_type,
    schoolNum,
    stateSchoolNum,
    name,
    phone,
    address,
    city,
    state,
    zip,
    fax AS principalFax,
    principal AS principalname,
    principal_email,
    courseCatalog,
    validation_errors
  FROM validated
),

data_rows AS (
  SELECT
    ROW_NUMBER() OVER (ORDER BY row_type, TO_NUMBER(REGEXP_SUBSTR(schoolNum,'\d+'))) AS data_order,
    f.*
  FROM final_data f
)

SELECT line
FROM (
  SELECT
    0 AS sort_order,
    0 AS data_order,
    '"row_type","schoolNum","stateSchoolNum","name","phone","address","city","state","zip","fax","principalName","principalEmail","courseCatalog","validation_errors"' AS line
  FROM dual

  UNION ALL

  SELECT
    1 AS sort_order,
    data_order,
    '"'||row_type||'","'||
    schoolNum||'","'||
    stateSchoolNum||'","'||
    name||'","'||
    phone||'","'||
    address||'","'||
    city||'","'||
    state||'","'||
    zip||'","'||
    principalFax||'","'||
    principalname||'","'||
    principal_email||'","'||
    courseCatalog||'","'||
    validation_errors||'"'
  FROM data_rows
)
ORDER BY sort_order, data_order;