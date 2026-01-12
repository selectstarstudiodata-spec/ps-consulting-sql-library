# ps-consulting-sql-library
Portfolio-ready PowerSchool SQL library demonstrating reusable enrollment logic, roster extracts, and SIS data validation with clear documentation and safe practices.
Reusable SQL patterns and extract logic for PowerSchool consulting work.

## Purpose
This repository demonstrates how I structure, document, and reason through
PowerSchool SQL used for reporting, data validation, and SIS integrations.

This is a **portfolio repository**. All logic is representative and sanitized.
No real district data, credentials, or proprietary vendor specifications
are included.

## What this repo contains
- Reusable SQL patterns for common PowerSchool use cases
- Parameterized extract logic (roster, attendance, enrollment)
- Data validation queries to identify SIS issues
- Clear documentation of assumptions and business rules

## What this repo does NOT contain
- Student or staff PII
- District-specific credentials or connection details
- NDA-restricted state or vendor extracts

## Design principles
- SQL is written to be readable first, clever second
- Business logic is documented explicitly
- Queries are designed to be reused across districts
- Parameters are used instead of hard-coded values

## Status
This library is actively evolving as patterns are refined and generalized.
