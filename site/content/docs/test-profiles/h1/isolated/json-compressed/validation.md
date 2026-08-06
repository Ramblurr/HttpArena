---
title: Validation
seo_title: "Compressed JSON Benchmark (gzip and Brotli) — Validation Checks"
description: "The correctness checks validate.sh runs against the compressed JSON benchmark before a framework's results are accepted."
---

The validation script (`scripts/validate.sh`) runs these checks for the `json-comp` test profile. All must pass for a framework to be considered valid for this benchmark.

## Checks

### Content-Encoding is set when Accept-Encoding is sent

```
GET /json/50?m=1
Accept-Encoding: gzip, br
```

The response must include `Content-Encoding: gzip` or `Content-Encoding: br`. Any other value (including absent) is a failure.

### Response body is correct for multiple (count, m) pairs

Three requests are sent with different counts and multipliers:

| Count | Multiplier |
|-------|-----------|
| 25 | 3 |
| 40 | 7 |
| 50 | 2 |

The optional `m` query parameter is an integer and defaults to integer `1` when absent. The table lists every `(count, m)` pair used for compressed-response validation.

For each response, after decompressing, the validator checks:

1. The response `count` is an integer equal to the route count, and `items` contains exactly that many entries
2. Every item has the documented types: `id`, `price`, `quantity`, `rating.score`, `rating.count`, and `total` are integers; `name` and `category` are strings; `tags` is an array of strings; and `active` is a boolean
3. Boolean values are rejected for integer fields
4. Each `total` equals the integer product `price * quantity * m`, with no rounding

Missing fields, invalid types, forged counts, or incorrect arithmetic fail validation after decompression.

### No Content-Encoding when Accept-Encoding is absent

```
GET /json/50?m=1
```

Without `Accept-Encoding`, the response **must not** include a `Content-Encoding` header. Compression is driven per request by the client - servers that unconditionally compress fail this check.

## Running locally

```bash
./scripts/validate.sh <framework>
```

Filter to this profile only:

```bash
./scripts/validate.sh <framework> json-comp
```
