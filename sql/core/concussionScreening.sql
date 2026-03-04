/* ============================================================================
  ETL: PowerSchool > Infinite Campus
  Target file: concussionScreening.csv
============================================================================ */

SELECT csv_line " "
FROM (

    /* ============================ HEADER */
    SELECT
        0 AS sort_order,
        '"recordType","studentNum","date","validation_errors"' AS csv_line
    FROM dual

    UNION ALL

    /* ============================ DATA */
    SELECT
        1 AS sort_order,

        '"' ||
        CASE
            WHEN src.screening_date IS NULL THEN 'ERROR'
            ELSE 'CLEAN'
        END || '","' ||

        REPLACE(src.studentNum,'"','""') || '","' ||

        NVL(TO_CHAR(src.screening_date,'YYYY-MM-DD'),'') || '","' ||

        CASE
            WHEN src.screening_date IS NULL THEN 'ERR_REQUIRED_date;'
            ELSE ''
        END || '"'

    FROM (

        SELECT DISTINCT
            CAST(s.student_number AS VARCHAR2(15)) AS studentNum,
            CAST(NULL AS DATE) AS screening_date
        FROM ps.students s
        JOIN ps.cc cc
            ON cc.studentid = s.id
        WHERE cc.termid IS NOT NULL
          AND s.student_number IS NOT NULL

    ) src

    UNION ALL

    /* ============================ FALLBACK */
    SELECT
        2 AS sort_order,
        '"ERROR","","","ERR_NO_SOURCE_ROWS_FOR_CONCUSSIONSCREENING;"'
    FROM dual
    WHERE NOT EXISTS (
        SELECT 1
        FROM ps.students s
        JOIN ps.cc cc
          ON cc.studentid = s.id
        WHERE cc.termid IS NOT NULL
          AND s.student_number IS NOT NULL
    )

)
ORDER BY sort_order;