---
title: Validation
seo_title: "JSON over TLS Benchmark — Validation Checks"
description: "The correctness checks validate.sh runs against the JSON over TLS benchmark before a framework's results are accepted."
---

The validation script (`scripts/validate.sh`) runs these checks for the `json-tls` test profile. All must pass for a framework to be considered valid for this benchmark.

## Checks

### ALPN negotiates HTTP/1.1

```
curl -sk --http1.1 https://localhost:8081/json/1?m=1
```

The response must report `http_version = 1.1`. A framework that advertises only `h2` on port 8081, or that refuses HTTP/1.1 clients, fails this check.

### Response body is correct for multiple (count, m) pairs

Three requests are sent over HTTPS on port 8081 with different counts and multipliers:

| Count | Multiplier |
|-------|-----------|
| 7 | 2 |
| 23 | 11 |
| 50 | 1 |

For each response the validator checks:

1. The response `count` field equals the route count
2. Every item contains the required fields; `tags` is an array, `active` is a boolean, and `rating` is an object containing `score` and `count`
3. Each numeric `total` equals `price * quantity * m`

The endpoint contract separately requires exactly `count` items and an integer `total` computed with no rounding. These checks currently verify the response count field, required structure, and numeric arithmetic equality. The `(count, m)` pairs are deliberately **different** from the `json-comp` validation pairs so a framework that tries to cache validation results across profiles cannot pass both.

### Content-Type is application/json

```
curl -sk -D- -o /dev/null https://localhost:8081/json/1?m=1
```

The response must include a `Content-Type` header containing `application/json`.

## Running locally

```bash
./scripts/validate.sh <framework>
```

Filter to this profile only:

```bash
./scripts/validate.sh <framework> json-tls
```
