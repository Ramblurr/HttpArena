---
title: Validation
seo_title: "JSON Processing Benchmark — Validation Checks"
description: "The correctness checks validate.sh runs against the JSON processing benchmark before a framework's results are accepted."
---

The following checks are executed by `validate.sh` for every framework subscribed to the `json` test.

## Response structure and computed totals

Sends `GET /json/{count}?m={multiplier}` for `(count, m)` pairs **`(12,3)`, `(22,7)`, `(31,2)`, and `(50,5)`** (different from the benchmark pairs to prevent hardcoded responses). The optional `m` query parameter is an integer and defaults to integer `1` when absent. For each request, validation verifies:

- The response `count` field equals the requested count
- Every item contains the required fields; `tags` is an array, `active` is a boolean, and `rating` is an object containing `score` and `count`
- Each numeric `total` equals `price * quantity * m`

The endpoint contract separately requires exactly `count` items and an integer `total` computed with no rounding. These checks currently verify the response count field, required structure, and numeric arithmetic equality.

## Content-Type header

Sends `GET /json/50?m=1` and verifies the `Content-Type` response header is `application/json`.
