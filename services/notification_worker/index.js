/**
 * Notification system — Cloudflare Workers (FaaS) + Telegram + KV.
 *
 * Two public endpoints (deployed at e.g. election-notifications.<account>.workers.dev):
 *  - POST /notify     — the anomaly engines push anomalies here
 *                       (→ sent to the Telegram group AND stored in KV)
 *  - GET /anomalies   — token-protected; lets the Render dashboard API read the
 *                       anomaly history without ever touching Kafka
 *
 * Deploy:
 *   npx wrangler login
 *   npx wrangler kv namespace create ANOMALIES     # put id in wrangler.toml
 *   npx wrangler secret put TELEGRAM_BOT_TOKEN
 *   npx wrangler secret put TELEGRAM_CHAT_ID
 *   npx wrangler secret put LIST_TOKEN
 *   npx wrangler deploy
 */

const ANOMALY_EMOJI = {
  duplicate_report: "🔁",
  turnout_above_100: "🗳️",
  late_submission: "⏰",
  impossible_jump: "📈",
  totals_mismatch: "🧮",
};

const KV_LIST = "anomalies_list";        // KV key holding the last 50 anomalies (JSON array)

async function pushAnomaly(env, anomaly) {
  const list = JSON.parse((await env.ANOMALIES.get(KV_LIST)) ?? "[]");
  list.unshift(anomaly);
  await env.ANOMALIES.put(KV_LIST, JSON.stringify(list.slice(0, 50)));
}

async function sendTelegram(env, anomaly) {
  const emoji = ANOMALY_EMOJI[anomaly.kind] ?? "🚨";
  const text =
    `${emoji} *Anomalie détectée — ${anomaly.kind}*\n` +
    `• bureau \`${anomaly.station_id}\` (région \`${anomaly.region || "?"}\`)\n` +
    `• ${anomaly.details}\n` +
    `• ${anomaly.detected_at ?? new Date().toISOString()}`;
  await fetch(`https://api.telegram.org/bot${env.TELEGRAM_BOT_TOKEN}/sendMessage`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ chat_id: env.TELEGRAM_CHAT_ID, text, parse_mode: "Markdown" }),
  });
}

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);

    // ==== POST /notify — anomaly engines ====
    if (request.method === "POST" && url.pathname === "/notify") {
      const anomaly = await request.json();
      await ctx.waitUntil(Promise.allSettled([pushAnomaly(env, anomaly), sendTelegram(env, anomaly)]));
      return Response.json({ status: "notified" });
    }

    // ==== GET /anomalies — Render dashboard API (token) ====
    if (request.method === "GET" && url.pathname === "/anomalies") {
      if (!env.LIST_TOKEN || request.headers.get("X-List-Token") !== env.LIST_TOKEN) {
        return new Response("unauthorized", { status: 401 });
      }
      const anomalies = JSON.parse((await env.ANOMALIES.get(KV_LIST)) ?? "[]");
      return Response.json({ count: anomalies.length, anomalies });
    }

    return new Response("projet élections notification worker\n", { status: 404 });
  },
};
