---
title: Upload
seo_title: "Large Upload Benchmark — Rotating Payload Sizes"
description: "Measures large request body ingestion by rotating 500 KB, 2 MB, 10 MB, and 20 MB payloads and returning each exact byte count."
---

Measures how efficiently a framework handles large request body ingestion. Requests rotate through 500 KB, 2 MB, 10 MB, and 20 MB binary payloads; the server reads each body and returns its exact byte count.

{{< cards >}}
  {{< card link="implementation" title="Implementation Guidelines" subtitle="Endpoint specification, expected request/response format, and type-specific rules." icon="code" >}}
  {{< card link="validation" title="Validation" subtitle="All checks executed by the validation script for this test profile." icon="check-circle" >}}
{{< /cards >}}
