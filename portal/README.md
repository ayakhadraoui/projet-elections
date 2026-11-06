# Project Élections — submission portal (Vercel)

Minimal static portal: polling stations type in a report, it is validated
client-side (integers only, no decimals, no negatives) then forwarded to the
Ingestion API on Render.

Vercel auto-publishes static files. Backend URL: set it as the constant
`INGEST_API_URL` below (or via environment at build time later).
