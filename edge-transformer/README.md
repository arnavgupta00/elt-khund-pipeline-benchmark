# Edge Transformer Worker

This Cloudflare Worker transforms user records for Case 4a.

Configuration:
- Set secret auth token used by the Node caller:
  - wrangler secret put WORKER_AUTH_TOKEN
  - In Node, set CLOUDFLARE_WORKER_TOKEN to the same value.
- Deploy and get the Worker URL, then set CLOUDFLARE_WORKER_URL in your environment.

Optional tuning from Node process:
- CASE4A_BATCH_SIZE (default 1000)
- CASE4A_WORKER_CONCURRENCY (default 50)
- CASE4A_HTTP_TIMEOUT_MS (default 30000)
