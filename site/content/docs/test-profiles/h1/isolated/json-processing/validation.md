---
title: Validation
seo_title: "JSON Processing Benchmark — Validation Checks"
description: "The correctness checks validate.sh runs against the JSON processing benchmark before a framework's results are accepted."
---

The following checks are executed by `validate.sh` for every framework subscribed to the `json` test.

## Response structure and computed totals

Sends `GET /json/{count}?m={multiplier}` for `(count, m)` pairs **`(12,3)`, `(22,7)`, `(31,2)`, and `(50,5)`** (different from the benchmark pairs to prevent hardcoded responses). The optional `m` query parameter is an integer and defaults to integer `1` when absent. For each request, validation verifies:

- The response `count` is an integer equal to the requested count, and `items` contains exactly that many entries
- Every item has the documented types: `id`, `price`, `quantity`, `rating.score`, `rating.count`, and `total` are integers; `name` and `category` are strings; `tags` is an array of strings; and `active` is a boolean
- Boolean values are rejected for integer fields
- Each `total` equals the integer product `price * quantity * m`, with no rounding

## Content-Type header

Sends `GET /json/50?m=1` and verifies the `Content-Type` response header is `application/json`.
