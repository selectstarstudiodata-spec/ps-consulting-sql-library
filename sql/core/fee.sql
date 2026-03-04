/* ============================================================================
  ETL: PowerSchool > Infinite Campus
  Target file: fee.csv
============================================================================ */

set heading off
set pagesize 0
set feedback off
set underline off

WITH active_year AS (
  SELECT MAX(t.yearid) AS active_yearid
  FROM ps.sections sec
  JOIN ps.terms t ON t.id = sec.termid
),

student_scope AS (
  SELECT DISTINCT
    s.id AS studentid,
    CAST(s.student_number AS VARCHAR2(15)) AS studentNum
  FROM ps.students s
  JOIN ps.cc cc        ON cc.studentid = s.id
  JOIN ps.sections sec ON sec.id = cc.sectionid
  JOIN ps.terms t      ON t.id = sec.termid
  WHERE t.yearid = (SELECT active_yearid FROM active_year)
    AND s.student_number IS NOT NULL
),

ps_source AS (
  SELECT
    ss.studentNum,

    CAST(f.fee_type_name AS VARCHAR2(60)) AS feeName,
    CAST(f.fee_type_id AS VARCHAR2(10))   AS feeType,

    CAST(SUM(ft.neteffect) AS NUMBER(10,2)) AS amount

  FROM ps.fee_transaction ft
  JOIN ps.fee f
    ON f.id = ft.feeid
  JOIN student_scope ss
    ON ss.studentid = ft.studentid

  GROUP BY
    ss.studentNum,
    f.fee_type_name,
    f.fee_type_id
),

filtered AS (
  SELECT *
  FROM ps_source
  WHERE amount <> 0
),

validated AS (
  SELECT
    studentNum,
    feeName,
    feeType,
    amount,
    TRIM(
      CASE WHEN feeName IS NULL OR TRIM(feeName) = '' THEN 'ERR_REQUIRED_feeName; ' END ||
      CASE WHEN feeType IS NULL OR TRIM(feeType) = '' THEN 'ERR_REQUIRED_feeType; ' END ||
      CASE WHEN amount IS NULL THEN 'ERR_REQUIRED_amount; ' END
    ) AS validation_errors
  FROM filtered
),

final_rows AS (
  SELECT
    2 AS sort_order,
    CASE WHEN validation_errors IS NULL OR validation_errors = '' THEN 'CLEAN' ELSE 'ERROR' END AS row_type,
    studentNum,
    feeName,
    feeType,
    amount,
    validation_errors
  FROM validated

  UNION ALL

  SELECT
    2,
    'ERROR',
    CAST(NULL AS VARCHAR2(15)),
    CAST(NULL AS VARCHAR2(60)),
    CAST(NULL AS VARCHAR2(10)),
    CAST(NULL AS NUMBER(10,2)),
    'ERR_NO_SOURCE_ROWS_FOR_FEE; '
  FROM dual
  WHERE NOT EXISTS (SELECT 1 FROM validated)
)

SELECT csv_line
FROM (
  SELECT
    1 AS sort_order,
    '"row_type","studentNum","feeName","feeType","amount","validation_errors"' AS csv_line
  FROM dual

  UNION ALL

  SELECT
    sort_order,
    '"'||REPLACE(NVL(row_type,''),'"','""')||'",'|| 
    '"'||REPLACE(NVL(studentNum,''),'"','""')||'",'|| 
    '"'||REPLACE(NVL(feeName,''),'"','""')||'",'|| 
    '"'||REPLACE(NVL(feeType,''),'"','""')||'",'|| 
    '"'||NVL(TO_CHAR(amount,'FM999999990.00'),'')||'",'|| 
    '"'||REPLACE(NVL(validation_errors,''),'"','""')||'"'
  FROM final_rows
)
ORDER BY sort_order;