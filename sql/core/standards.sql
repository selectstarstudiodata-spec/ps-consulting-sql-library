/* ============================================================================
   ETL: PowerSchool -> Infinite Campus
   Target file: standard.csv

   District rule:
   - Send standards exactly as stored in PS.STANDARD
   - No truncation
   - No deduping
   - No renaming
============================================================================ */

set heading on
set pagesize 50000
set feedback off
set underline off
set trimspool on
set linesize 32767

WITH ps_source AS (
    SELECT s.*
    FROM ps.standard s
),

final_data AS (
    SELECT
        'CLEAN' AS recordType,
        s.*
    FROM ps_source s
)

SELECT
    'CLEAN' AS recordType,
    '"'||STANDARDID||'"' AS STANDARDID,
    '"'||PARENTSTANDARDID||'"' AS PARENTSTANDARDID,
    '"'||LONGITUDINALID||'"' AS LONGITUDINALID,
    '"'||YEARID||'"' AS YEARID,
    '"'||REPLACE(NVL(NAME,''),'"','""')||'"' AS NAME,
    '"'||REPLACE(NVL(DESCRIPTION,''),'"','""')||'"' AS DESCRIPTION,
    '"'||REPLACE(NVL(IDENTIFIER,''),'"','""')||'"' AS IDENTIFIER,
    '"'||REPLACE(NVL(SUBJECTAREA,''),'"','""')||'"' AS SUBJECTAREA,
    '"'||CONVERSIONSCALE||'"' AS CONVERSIONSCALE,
    '"'||GRADESCALEITEMDCID||'"' AS GRADESCALEITEMDCID,
    '"'||ISASSIGNMENTALLOWED||'"' AS ISASSIGNMENTALLOWED,
    '"'||ISACTIVE||'"' AS ISACTIVE,
    '"'||TO_CHAR(DEACTIVATEDATE,'YYYY-MM-DD HH24:MI:SS')||'"' AS DEACTIVATEDATE,
    '"'||DISPLAYPOSITION||'"' AS DISPLAYPOSITION,
    '"'||ISCOMMENTINCLUDED||'"' AS ISCOMMENTINCLUDED,
    '"'||MAXCOMMENTLENGTH||'"' AS MAXCOMMENTLENGTH,
    '"'||ISEXCLUDEDFROMREPORTS||'"' AS ISEXCLUDEDFROMREPORTS,
    '"'||REPLACE(NVL(IMPORTBATCHTRACKING,''),'"','""')||'"' AS IMPORTBATCHTRACKING,
    '"'||REPLACE(NVL(PSGUID,''),'"','""')||'"' AS PSGUID,
    '"'||REPLACE(NVL(WHOCREATED,''),'"','""')||'"' AS WHOCREATED,
    '"'||TO_CHAR(WHENCREATED,'YYYY-MM-DD HH24:MI:SS')||'"' AS WHENCREATED,
    '"'||REPLACE(NVL(WHOMODIFIED,''),'"','""')||'"' AS WHOMODIFIED,
    '"'||TO_CHAR(WHENMODIFIED,'YYYY-MM-DD HH24:MI:SS')||'"' AS WHENMODIFIED,
    '"'||REPLACE(NVL(TRANSIENTCOURSELIST,''),'"','""')||'"' AS TRANSIENTCOURSELIST,
    '"'||REPLACE(NVL(EXTERNALID,''),'"','""')||'"' AS EXTERNALID,
    '"'||PARENTSTANDARDID_NVL||'"' AS PARENTSTANDARDID_NVL
FROM final_data;