---
title: Database Concurrency (JDBC virtual threads)
seo_title: "Database Concurrency Benchmark (JDBC virtual threads)"
description: "Measures blocking JDBC queries scheduled on JDK 25 virtual threads with one bounded HikariCP datasource."
---

This profile measures concurrent blocking PostgreSQL JDBC requests scheduled on virtual threads. It is distinct from [Async Database](../async-database/): that profile requires a native asynchronous driver and records a separate result series.

{{< cards >}}
  {{< card link="implementation" title="Implementation Guidelines" subtitle="JDBC stack, endpoint contract, and pool bound." icon="code" >}}
  {{< card link="validation" title="Validation" subtitle="Functional checks and static contract validation." icon="check-circle" >}}
{{< /cards >}}
