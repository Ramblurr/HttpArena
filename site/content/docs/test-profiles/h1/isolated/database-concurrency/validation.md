---
title: Validation
seo_title: "Database Concurrency — JDBC virtual-thread validation"
description: "Functional checks and static contract validation for blocking JDBC on virtual threads."
---

`validate.sh` runs normal database response checks against `/database-concurrency`: varied ranges and limits, JSON content type, nested rating/tags/boolean values, and an empty range. It starts PostgreSQL with `DATABASE_MAX_CONN=256` for ordinary-container validation.

Before image build, `validate.sh` checks the static cohort contract:

- `meta.json` must subscribe to `database-concurrency` and declare `type: emerging`, `mode: tuned`.
- `frameworks/<framework>/database-concurrency.json` must exactly match the shared route, dependency, blocking-JDBC, pool, retry, and fallback contract in `data/database-concurrency-contract.json`.
- The framework must be one of the exact four initial cohort entries.
