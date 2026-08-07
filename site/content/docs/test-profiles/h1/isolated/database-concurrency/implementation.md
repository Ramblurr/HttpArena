---
title: Implementation Guidelines
seo_title: "Database Concurrency — JDBC virtual-thread implementation"
description: "Shared JDBC, HikariCP, JDK 25 virtual-thread, and endpoint rules for the Clojure database-concurrency cohort."
---

{{< type-rules standard="Not available for this initial cohort." tuned="Use the exact shared JDBC/HikariCP contract below; do not add framework-private throughput tuning." engine="No specific rules." >}}

The initial cohort contains http-kit, Pedestal, Ring, and ring-jetty9-adapter. Each entry is `type: emerging` and `mode: tuned`. This profile uses blocking JDBC with scalable virtual-thread concurrency. PGJDBC remains blocking; virtual threads do not make it native asynchronous I/O or increase PostgreSQL capacity.

## Endpoint and database work

Implement `GET /database-concurrency?min=X&max=Y&limit=N`.

- Defaults: `min=10`, `max=50`, and `limit=50`; parse all values as integers and clamp `limit` to 1–50.
- Query the existing `items` table with the parameterized SQL used by Async Database: select item fields where `price BETWEEN $1 AND $2 LIMIT $3`.
- Return JSON with `items`, integer fields, nested `rating: {score, count}`, and `count` derived from the returned rows.
- On pool creation, acquire/query, mapping, or serialization failure, return `200 {"items":[],"count":0}`. Keep the listener alive and let a later request retry.

## Required common stack

Use direct dependencies `next.jdbc 1.3.1118`, PGJDBC `42.7.13`, and HikariCP `4.1.0` on image-proven JDK 25. A process owns one shared datasource. Convert the runner `DATABASE_URL` to the JDBC URL without putting credentials in the URL, and give Hikari the decoded username and password separately.

Set Hikari's maximum pool size from `DATABASE_MAX_CONN` directly. Do not derive it from CPU count, request count, or virtual-thread count; add an application cache, warm-up policy, extra datasource, request-thread datasource close, or framework-private executor queue/sizing.

## Result separation and static contract

Keep `database-concurrency` results separate from `async-db` results. Do not describe PGJDBC as nonblocking or async.

The source tree checks each cohort entry's metadata and `frameworks/<framework>/database-concurrency.json` declaration against `data/database-concurrency-contract.json`. These checks keep profile, route, dependency, and pool settings consistent; normal validation and benchmarking need no external evidence directory.
