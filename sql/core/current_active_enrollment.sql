/*
Title: Current Active Enrollment Logic

Purpose:
Defines a reusable, district-agnostic pattern for identifying students
who are currently enrolled and active in PowerSchool.

This logic is intended to be referenced by extracts, reports, and
data validation queries.

Parameters:
- :as_of_date   DATE  -- The effective date for enrollment evaluation
- :school_id    INT   -- Optional filter for a specific school

Assumptions:
- Enrollment records with exit dates prior to :as_of_date are inactive
- Students flagged as inactive are excluded
- Future enrollments are excluded unless active on :as_of_date

Notes:
- This file defines logic only. It does not perform extraction.
- Field and table names are generalized for portfolio purposes.

Change Log:
- 2026-01-12 - Initial version
*/

SELECT
    s.student_id,
    s.school_id,
    s.grade_level,
    s.enroll_date,
    s.exit_date
FROM students s
WHERE
    s.enroll_date <= :as_of_date
    AND (
        s.exit_date IS NULL
        OR s.exit_date >= :as_of_date
    )
    AND s.active_flag = 1
    AND (
        :school_id IS NULL
        OR s.school_id = :school_id
    );
