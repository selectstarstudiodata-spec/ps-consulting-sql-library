set heading off
set pagesize 0
set feedback off
set underline off
set trimspool on
set linesize 32767
set newpage none

/* ============================================================================
   ETL: PowerSchool > Infinite Campus
   Target file: behaviorRole.csv
============================================================================ */

WITH has_rows AS (
  SELECT
    CASE
      WHEN EXISTS (SELECT 1 FROM ps.incident_person_role WHERE ROWNUM = 1)
      THEN 1 ELSE 0
    END AS has_role_rows
  FROM dual
),

ps_source AS (
  SELECT DISTINCT
    SUBSTR(
      TRIM(
        COALESCE(
          NULLIF(TRIM(ipr.person_desc), ''),
          'INCIDENT_PERSON_ROLE'
        )
      ),
      1,
      50
    ) AS roleName
  FROM ps.incident_person_role ipr
  WHERE (SELECT has_role_rows FROM has_rows) = 1
),

validated AS (
  SELECT
    roleName,
    TRIM(
      CASE
        WHEN roleName IS NULL OR TRIM(roleName) = ''
        THEN 'ERR_REQUIRED_roleName; '
      END
    ) AS validation_errors
  FROM ps_source
),

real_rows AS (
  SELECT
    CASE
      WHEN validation_errors IS NULL OR validation_errors = ''
      THEN 'CLEAN'
      ELSE 'ERROR'
    END AS row_type,
    roleName,
    validation_errors
  FROM validated
),

info_row AS (
  SELECT
    'INFO' AS row_type,
    CAST(NULL AS VARCHAR2(50)) AS roleName,
    'NO_SOURCE_ROWS_IN_PS.INCIDENT_PERSON_ROLE' AS validation_errors
  FROM dual
  WHERE (SELECT has_role_rows FROM has_rows) = 0
),

final_output AS (
  SELECT * FROM real_rows
  UNION ALL
  SELECT * FROM info_row
)

SELECT csv_line
FROM (
  SELECT 0 AS sort_order, 0 AS data_order,
         '"row_type","roleName","validation_errors"' AS csv_line
  FROM dual

  UNION ALL

  SELECT
    1,
    ROW_NUMBER() OVER (ORDER BY row_type DESC, roleName),

    '"'||REPLACE(NVL(row_type,''),'"','""')||'",'||
    '"'||REPLACE(NVL(roleName,''),'"','""')||'",'||
    '"'||REPLACE(NVL(validation_errors,''),'"','""')||'"'

  FROM final_output
)
ORDER BY sort_order, data_order;