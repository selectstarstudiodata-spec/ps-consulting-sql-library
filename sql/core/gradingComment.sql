set heading off
set pagesize 0
set feedback off
set underline off
set trimspool on
set termout off

/* ============================================================
   Grading Comment Extract – Batch 2
   Standardized Output:
   - Header fully quoted
   - All data fields quoted
   - Comma separated
   - No leading space
   ============================================================ */

SELECT csv_line
FROM (

    /* =========================
       HEADER ROW
       ========================= */
    SELECT
        0 AS sort_order,
        '"row_type","schoolNum","code","comment","validation_errors"' AS csv_line
    FROM dual

    UNION ALL

    /* =========================
       DATA ROWS
       ========================= */
    SELECT
        1 AS sort_order,
        '"'||row_type||'",'||
        '"'||NVL(schoolNum,'')||'",'||
        code_q||','||
        comment_q||','||
        validation_errors_q AS csv_line
    FROM (

        /* =====================================================
           ACTIVE YEAR
           ===================================================== */
        SELECT
            CASE
                WHEN validation_errors IS NULL OR validation_errors = '' THEN 'CLEAN'
                ELSE 'ERROR'
            END AS row_type,
            schoolNum,
            '"' || REPLACE(REPLACE(REPLACE(NVL(code,''), '"','""'),CHR(13),' '),CHR(10),' ') || '"' AS code_q,
            '"' || REPLACE(REPLACE(REPLACE(NVL(comment_txt,''),'"','""'),CHR(13),' '),CHR(10),' ') || '"' AS comment_q,
            '"' || REPLACE(REPLACE(REPLACE(NVL(validation_errors,''),'"','""'),CHR(13),' '),CHR(10),' ') || '"' AS validation_errors_q
        FROM (

            /* ================================================
               VALIDATED + DEDUPED
               ================================================ */
            SELECT *
            FROM (
                SELECT
                    schoolNum_raw AS schoolNum,
                    code_raw      AS code,
                    SUBSTR(TRIM(comment_raw),1,200) AS comment_txt,

                    TRIM(
                        CASE
                            WHEN comment_raw IS NULL OR TRIM(comment_raw) = ''
                            THEN 'ERR_REQUIRED_comment; '
                        END
                    ) AS validation_errors,

                    ROW_NUMBER() OVER (
                        PARTITION BY NVL(schoolNum_raw,'~'),
                                     SUBSTR(TRIM(comment_raw),1,200)
                        ORDER BY code_raw NULLS LAST
                    ) rn
                FROM (

                    /* =========================================
                       SOURCE UNION
                       ========================================= */

                    /* Section Comments */
                    SELECT
                        CAST(NULL AS VARCHAR2(7)) AS schoolNum_raw,
                        CAST(NULL AS VARCHAR2(6)) AS code_raw,
                        CAST(s.commentvalue AS VARCHAR2(4000)) AS comment_raw
                    FROM ps.standardgradesectioncomment s
                    WHERE s.commentvalue IS NOT NULL
                      AND TRIM(s.commentvalue) <> ''

                    UNION ALL

                    /* Rollup Comments */
                    SELECT
                        CAST(NULL AS VARCHAR2(7)),
                        CAST(NULL AS VARCHAR2(6)),
                        CAST(DBMS_LOB.SUBSTR(r.commentvalue,4000,1) AS VARCHAR2(4000))
                    FROM ps.standardgraderollupcomment r
                    WHERE r.commentvalue IS NOT NULL
                      AND TRIM(DBMS_LOB.SUBSTR(r.commentvalue,4000,1)) <> ''

                    UNION ALL

                    /* Stored Grade Comments – Active Year Only */
                    SELECT
                        CAST(NULL AS VARCHAR2(7)),
                        CAST(NULL AS VARCHAR2(6)),
                        CAST(DBMS_LOB.SUBSTR(sg.comment_value,4000,1) AS VARCHAR2(4000))
                    FROM ps.storedgrades sg
                    JOIN ps.terms t
                      ON t.id = sg.termid
                     AND t.schoolid = sg.schoolid
                    WHERE sg.comment_value IS NOT NULL
                      AND TRIM(DBMS_LOB.SUBSTR(sg.comment_value,4000,1)) <> ''
                      AND t.yearid = (
                            SELECT yearid
                            FROM (
                                SELECT
                                    t2.yearid,
                                    ROW_NUMBER() OVER (
                                        ORDER BY
                                            CASE
                                                WHEN SYSDATE BETWEEN t2.firstday AND t2.lastday
                                                THEN 0 ELSE 1
                                            END,
                                            NVL(t2.lastday, DATE '1900-01-01') DESC,
                                            t2.id DESC
                                    ) rn
                                FROM ps.terms t2
                                WHERE t2.isyearrec = 1
                            )
                            WHERE rn = 1
                      )

                )
            )
            WHERE rn = 1
        )
    )

    UNION ALL

    /* =========================
       PLACEHOLDER IF NO DATA
       ========================= */
    SELECT
        2 AS sort_order,
        '"ERROR","","","NO_COMMENTS_FOUND","WARN_NO_ROWS_RETURNED"'
    FROM dual
    WHERE NOT EXISTS (
        SELECT 1
        FROM ps.standardgradesectioncomment
        WHERE ROWNUM = 1
    )

)
ORDER BY sort_order;