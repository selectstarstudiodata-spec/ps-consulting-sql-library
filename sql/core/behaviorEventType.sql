set heading off
set feedback off
set pagesize 0
set verify off
set trimspool on
set underline off
set newpage none

/* ============================================================================
   ETL: PowerSchool > Infinite Campus
   Target file: behaviorEventType.csv
============================================================================ */

WITH
log_subjects AS (
  SELECT DISTINCT
    SUBSTR(
      TRIM(
        COALESCE(
          NULLIF(TRIM(li.subject), ''),
          'LOGTYPE_' || TO_CHAR(li.logtypeid),
          'UNKNOWN'
        )
      ),
      1,
      100
    ) AS eventName
  FROM ps.log li
),

gen_logtypes AS (
  SELECT DISTINCT
    SUBSTR(
      TRIM(
        COALESCE(
          NULLIF(TRIM(g.value), ''),
          NULLIF(TRIM(g.name), ''),
          'LOGTYPE_' || TO_CHAR(g.id),
          'UNKNOWN'
        )
      ),
      1,
      100
    ) AS eventName
  FROM ps.gen g
  WHERE g.cat = 'logtype'
),

ps_source AS (
  SELECT eventName FROM log_subjects
  UNION
  SELECT eventName FROM gen_logtypes
),

validated AS (
  SELECT
    s.eventName,
    CASE
      WHEN s.eventName IS NULL OR TRIM(s.eventName) = '' THEN 'ERR_REQUIRED_eventName; '
      ELSE ''
    END AS validation_errors
  FROM ps_source s
)

SELECT csv_line
FROM (
  SELECT 0 AS sort_order, 0 AS data_order,
         '"row_type","eventName","validation_errors"' AS csv_line
  FROM dual

  UNION ALL

  SELECT 1,
         ROW_NUMBER() OVER (ORDER BY eventName),

         '"'||
         CASE
           WHEN validation_errors IS NULL OR validation_errors = ''
           THEN 'CLEAN'
           ELSE 'ERROR'
         END||'",'||

         '"'||REPLACE(NVL(eventName,''),'"','""')||'",'||
         '"'||REPLACE(NVL(validation_errors,''),'"','""')||'"'

  FROM validated
)
ORDER BY sort_order, data_order;