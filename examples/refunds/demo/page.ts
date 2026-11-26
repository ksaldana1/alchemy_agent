// The demo's page, served at `/` by demo/platform.ts. Plain HTML and JavaScript that poll the HTTP API.
// Kept free of backticks and `${` so it can live in this string.
export const page = /* html */ `
<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Refund desk</title>
<style>
  :root {
    --bg: #f6f5f2; --panel: #ffffff; --ink: #1d1c1a; --muted: #6b6860; --line: #e4e1da;
    --accent: #2f5bd3; --ok: #1f7a4d; --ok-bg: #e6f4ec; --warn: #9a5b00; --warn-bg: #fdf1dc;
    --bad: #b42318; --bad-bg: #fde8e6; --wait: #6941c6; --wait-bg: #efe9fb;
    --mono: ui-monospace, SFMono-Regular, Menlo, monospace;
  }
  @media (prefers-color-scheme: dark) {
    :root {
      --bg: #151513; --panel: #1e1e1b; --ink: #ecebe7; --muted: #9c998f; --line: #33322e;
      --accent: #7d9cf0; --ok: #5fcf95; --ok-bg: #1b3326; --warn: #f0b252; --warn-bg: #3a2c14;
      --bad: #f2867b; --bad-bg: #3d1e1b; --wait: #b9a2f2; --wait-bg: #2b2340;
    }
  }
  * { box-sizing: border-box; }
  body { margin: 0; background: var(--bg); color: var(--ink); font: 14px/1.45 system-ui, -apple-system, "Segoe UI", sans-serif; }
  header { padding: 20px 24px 8px; display: flex; flex-wrap: wrap; gap: 12px; align-items: baseline; justify-content: space-between; }
  h1 { font-size: 20px; margin: 0; }
  header p { margin: 4px 0 0; color: var(--muted); max-width: 70ch; }
  .token { font-size: 12px; color: var(--muted); }
  .token input { width: 120px; font: 12px var(--mono); }
  main { display: grid; grid-template-columns: 320px minmax(0, 1fr) 320px; gap: 16px; padding: 12px 24px 32px; align-items: start; }
  @media (max-width: 1100px) { main { grid-template-columns: 1fr; padding: 12px 16px 32px; } }
  section { background: var(--panel); border: 1px solid var(--line); border-radius: 10px; padding: 16px; }
  section + section { margin-top: 16px; }
  h2 { font-size: 13px; text-transform: uppercase; letter-spacing: .06em; color: var(--muted); margin: 0 0 12px; }
  label { display: block; font-size: 12px; color: var(--muted); margin: 10px 0 4px; }
  input[type=text], input[type=number], textarea { width: 100%; padding: 7px 9px; border: 1px solid var(--line); border-radius: 6px; background: var(--bg); color: var(--ink); font: inherit; }
  textarea { resize: vertical; min-height: 54px; }
  button { font: inherit; border-radius: 6px; border: 1px solid var(--line); background: var(--panel); color: var(--ink); padding: 6px 10px; cursor: pointer; }
  button:hover { border-color: var(--muted); }
  button.primary { background: var(--accent); border-color: var(--accent); color: #fff; width: 100%; padding: 9px; margin-top: 14px; font-weight: 600; }
  button.ok { color: var(--ok); } button.bad { color: var(--bad); }
  .presets { display: flex; flex-wrap: wrap; gap: 6px; }
  .presets button { font-size: 12px; }
  .chaos { display: flex; gap: 8px; align-items: flex-start; margin-top: 14px; padding: 10px; border-radius: 8px; background: var(--bad-bg); color: var(--bad); font-size: 13px; cursor: pointer; }
  .chaos input { margin-top: 3px; }
  .order { border: 1px solid var(--line); border-radius: 8px; padding: 12px; }
  .order + .order { margin-top: 10px; }
  .order-head { display: flex; gap: 8px; align-items: baseline; justify-content: space-between; }
  .order-head strong { font-size: 15px; }
  .meta { color: var(--muted); font-size: 12px; }
  .reason { color: var(--muted); font-style: italic; margin: 4px 0 8px; }
  .pill { display: inline-block; font-size: 11px; font-weight: 600; padding: 2px 8px; border-radius: 99px; white-space: nowrap; }
  .pill.completed, .pill.refunded { background: var(--ok-bg); color: var(--ok); }
  .pill.declined, .pill.failed { background: var(--bad-bg); color: var(--bad); }
  .pill.waiting { background: var(--wait-bg); color: var(--wait); }
  .pill.pending, .pill.running { background: var(--warn-bg); color: var(--warn); }
  ol.steps { list-style: none; margin: 0; padding: 0; }
  ol.steps li { display: grid; grid-template-columns: 22px 1fr auto; gap: 6px; padding: 5px 0; border-top: 1px dashed var(--line); align-items: baseline; }
  .note { grid-column: 2 / 4; font-size: 12px; color: var(--muted); }
  .note.crash { color: var(--bad); }
  .mono { font-family: var(--mono); font-size: 12px; }
  .empty { color: var(--muted); font-size: 13px; }
  .request { border: 1px solid var(--wait); border-radius: 8px; padding: 10px; background: var(--wait-bg); }
  .request + .request { margin-top: 8px; }
  .request p { margin: 0 0 8px; }
  .request .actions { display: flex; gap: 6px; }
  .stat { display: flex; justify-content: space-between; align-items: baseline; padding: 6px 0; border-top: 1px solid var(--line); }
  .stat:first-of-type { border-top: 0; }
  .stat b { font-size: 22px; font-variant-numeric: tabular-nums; }
  .callout { margin-top: 10px; padding: 10px; border-radius: 8px; background: var(--ok-bg); color: var(--ok); font-size: 13px; }
  ul.ledger { list-style: none; margin: 8px 0 0; padding: 0; font-size: 12px; }
  ul.ledger li { display: flex; justify-content: space-between; gap: 8px; padding: 3px 0; }
  .toast { position: fixed; bottom: 16px; left: 50%; transform: translateX(-50%); background: var(--ink); color: var(--bg); padding: 8px 14px; border-radius: 8px; font-size: 13px; opacity: 0; transition: opacity .2s; pointer-events: none; }
  .toast.show { opacity: 1; }
  .error { color: var(--bad); font-size: 12px; margin-top: 8px; }
</style>
</head>
<body>
<header>
  <div>
    <h1>Refund desk</h1>
    <p>Three agents on Durable Objects. Ask for a refund, crash the Payments agent at the worst moment, approve
      the big ones, send duplicates. Then check the ledger: every refund and email happened once.</p>
  </div>
  <div class="token">API token <input id="token" type="text" spellcheck="false"></div>
</header>

<main>
  <div>
    <section>
      <h2>Customer</h2>
      <div class="presets" id="presets"></div>
      <form id="form">
        <label for="item">Item</label>
        <input id="item" type="text" required>
        <label for="amount">Amount ($)</label>
        <input id="amount" type="number" min="1" step="0.01" required>
        <label for="reason">Reason</label>
        <textarea id="reason" required></textarea>
        <label class="chaos"><input id="crash" type="checkbox" checked>
          <span><b>Crash Payments after the gateway moves the money</b>, before it records the refund. The worst moment to die.</span></label>
        <button class="primary" type="submit">Request refund</button>
        <div class="error" id="form-error"></div>
      </form>
    </section>
  </div>

  <section>
    <h2>Orders</h2>
    <div id="orders"><p class="empty">No requests yet. Pick a preset and send one.</p></div>
  </section>

  <div>
    <section>
      <h2>Manager's queue</h2>
      <div id="requests"><p class="empty">Nothing to approve. Refunds over $200 land here.</p></div>
    </section>
    <section>
      <h2>Outside world</h2>
      <div id="ledger"></div>
    </section>
  </div>
</main>
<div class="toast" id="toast"></div>

<script>
const PRESETS = [
  { label: "Kettle · $34", item: "Pour-over kettle", amount: 34, reason: "It arrived with a cracked handle." },
  { label: "Espresso · $649", item: "Espresso machine", amount: 649, reason: "The pump stopped working after a week." },
  { label: "Throw · $89", item: "Linen throw", amount: 89, reason: "I changed my mind, it's been about two months." },
];
const RUN = Math.random().toString(36).slice(2, 5);
let nextOrder = 1042;

const $ = (id) => document.getElementById(id);
const store = { get: (k) => { try { return localStorage.getItem(k); } catch { return null; } }, set: (k, v) => { try { localStorage.setItem(k, v); } catch {} } };
const hashToken = new URLSearchParams(location.hash.slice(1)).get("token");
$("token").value = hashToken || store.get("refunds-token") || "demo-token";
$("token").addEventListener("change", () => store.set("refunds-token", $("token").value));
if (hashToken) store.set("refunds-token", hashToken);

function h(tag, attrs, ...children) {
  const el = document.createElement(tag);
  for (const [k, v] of Object.entries(attrs || {})) {
    if (k === "class") el.className = v; else if (k.startsWith("on")) el.addEventListener(k.slice(2), v); else el.setAttribute(k, v);
  }
  for (const c of children.flat()) if (c !== null && c !== undefined && c !== false) el.append(c instanceof Node ? c : String(c));
  return el;
}
const dollars = (cents) => "$" + (cents / 100).toFixed(2);
const pill = (cls, text) => h("span", { class: "pill " + cls }, text || cls);
function toast(text) { const t = $("toast"); t.textContent = text; t.classList.add("show"); clearTimeout(t._h); t._h = setTimeout(() => t.classList.remove("show"), 2600); }

async function api(method, path, body, text) {
  const res = await fetch(path, {
    method,
    headers: { authorization: "Bearer " + $("token").value, ...(body !== undefined && !text ? { "content-type": "application/json" } : {}) },
    body: body === undefined ? undefined : text ? body : JSON.stringify(body),
  });
  if (res.status === 401) throw new Error("The API token is wrong. It's AGENT_API_TOKEN (the demo uses demo-token).");
  const raw = await res.text();
  let data; try { data = JSON.parse(raw); } catch { data = raw; }
  if (!res.ok && res.status !== 202) { const e = new Error((data && data.message) || (data && data.error) || res.statusText); e.status = res.status; throw e; }
  return data;
}
const orEmpty = (p) => p.catch((e) => { if (e.status === 404 || e.status === 403) return []; throw e; });

// ---- the form -------------------------------------------------------------
function usePreset(p) { $("item").value = p.item; $("amount").value = p.amount; $("reason").value = p.reason; }
for (const p of PRESETS) $("presets").append(h("button", { type: "button", onclick: () => usePreset(p) }, p.label));
usePreset(PRESETS[0]);

async function sendRefund(payload, crash) {
  if (crash) await api("PUT", "/workspace/files/agents/Payments/acme-pay/chaos/" + payload.orderId, "armed", true);
  return api("POST", "/agents/RefundDesk/acme/tasks", { tag: "RequestRefund", taskId: "RequestRefund:" + payload.orderId, payload });
}

$("form").addEventListener("submit", async (event) => {
  event.preventDefault();
  $("form-error").textContent = "";
  const orderId = RUN + "-" + nextOrder++;
  const payload = {
    orderId, customer: "customer" + orderId.split("-")[1] + "@example.com",
    item: $("item").value, amount: Math.round(Number($("amount").value) * 100), reason: $("reason").value,
  };
  try {
    await sendRefund(payload, $("crash").checked);
    toast("Order " + orderId + " sent to RefundDesk/acme" + ($("crash").checked ? ", with a crash planted" : ""));
    refresh();
  } catch (e) { $("form-error").textContent = e.message; }
});

// ---- rendering ------------------------------------------------------------
const step = (icon, label, right, ...notes) => h("li", {}, h("span", {}, icon), h("span", {}, label), right || "", ...notes.filter(Boolean));

function renderOrder(task, children, requests, crashArmed) {
  const input = task.input || {};
  const result = task.status === "completed" ? task.result : null;
  const ownerId = "RefundDesk/acme/" + task.taskId;
  const mine = (list) => list.filter((t) => t.causation === ownerId);
  const payment = mine(children.payments)[0];
  const email = mine(children.mails)[0];
  const approval = requests.find((r) => r.causation === ownerId);
  const status = result ? result.status : task.status;
  const steps = [];

  steps.push(step("🧠", "Model reads the request", pill(result || payment || email || approval || task.status === "waiting" ? "completed" : "running", result || payment || email || approval || task.status === "waiting" ? "done" : "thinking")));
  if (approval) {
    const answer = approval.answer && approval.answer.response;
    steps.push(step("🙋", "Manager approval",
      approval.status === "pending" ? pill("waiting") : pill(answer && answer.approved ? "completed" : "declined", answer && answer.approved ? "approved" : approval.status === "resolved" ? "declined" : approval.status),
      approval.status === "pending" ? h("span", { class: "note" }, "RefundDesk's task is waiting. No code runs until someone answers.") : null));
  }
  if (payment) {
    steps.push(step("💳", h("span", {}, "Payments.IssueRefund ", h("span", { class: "meta" }, "× " + payment.attempts)), pill(payment.status),
      payment.attempts > 1 ? h("span", { class: "note crash" }, "💥 Died after the gateway refunded. Delivered again with the same task id, so the gateway returned the same refund.") :
      crashArmed && payment.status !== "completed" ? h("span", { class: "note crash" }, "💥 A crash is planted here.") : null));
  }
  if (email) {
    steps.push(step("✉️", h("span", {}, "Mailer.SendEmail ", h("span", { class: "meta" }, "× " + email.attempts)), pill(email.status)));
  }
  if (result) {
    steps.push(step(result.status === "refunded" ? "✅" : "🚫",
      result.status === "refunded" ? h("span", {}, "Refunded ", h("span", { class: "mono" }, result.refundId)) : "Declined, with an email to the customer",
      h("span", { class: "meta" }, "decided by " + result.decidedBy)));
  }

  return h("div", { class: "order" },
    h("div", { class: "order-head" },
      h("div", {}, h("strong", {}, input.item), " ", h("span", { class: "meta" }, dollars(input.amount) + " · order " + input.orderId)),
      pill(status)),
    h("div", { class: "reason" }, "“" + input.reason + "”"),
    h("ol", { class: "steps" }, steps),
    h("div", { style: "margin-top:8px" }, h("button", {
      type: "button",
      onclick: async () => { await sendRefund(input, false); toast("Sent order " + input.orderId + " again: same task id, so nothing new happens."); },
    }, "Send again (duplicate)")));
}

function renderRequests(requests) {
  const pending = requests.filter((r) => r.status === "pending");
  const root = $("requests"); root.replaceChildren();
  if (pending.length === 0) { root.append(h("p", { class: "empty" }, "Nothing to approve. Refunds over $200 land here.")); return; }
  for (const r of pending) {
    const answer = (approved) => async () => {
      await api("POST", "/requests/" + encodeURIComponent(r.id) + "/respond", { response: approved ? { approved: true } : { approved: false, notes: "declined in the demo" }, by: "manager" });
      toast(approved ? "Approved. RefundDesk wakes up and carries on." : "Declined.");
      refresh();
    };
    root.append(h("div", { class: "request" }, h("p", {}, r.prompt),
      h("div", { class: "actions" }, h("button", { class: "ok", onclick: answer(true) }, "Approve"), h("button", { class: "bad", onclick: answer(false) }, "Decline"))));
  }
}

const refundCache = new Map();
async function renderLedger(gatewayRequests, refundFiles, sent) {
  for (const name of refundFiles) {
    if (refundCache.has(name)) continue;
    const refund = await api("GET", "/workspace/files/agents/Payments/acme-pay/gateway/refunds/" + encodeURIComponent(name)).catch(() => null);
    if (refund) refundCache.set(name, refund);
  }
  const refunds = refundFiles.map((n) => refundCache.get(n)).filter(Boolean);
  const total = refunds.reduce((sum, r) => sum + r.amount, 0);
  const absorbed = gatewayRequests.length - refundFiles.length;
  $("ledger").replaceChildren(
    h("div", { class: "stat" }, h("span", {}, "Requests to the payment gateway"), h("b", {}, gatewayRequests.length)),
    h("div", { class: "stat" }, h("span", {}, "Refunds issued"), h("b", {}, refundFiles.length)),
    h("div", { class: "stat" }, h("span", {}, "Money refunded"), h("b", {}, dollars(total))),
    h("div", { class: "stat" }, h("span", {}, "Emails sent"), h("b", {}, sent.length)),
    absorbed > 0 ? h("div", { class: "callout" }, absorbed + (absorbed === 1 ? " request was a redelivery" : " requests were redeliveries") +
      " after a crash. Each one carried its task's id as the idempotency key, so the gateway refunded nothing twice.") : null,
    refunds.length ? h("ul", { class: "ledger" }, refunds.map((r) => h("li", {}, h("span", { class: "mono" }, r.refundId), h("span", {}, "order " + r.orderId), h("span", {}, dollars(r.amount))))) : null,
  );
}

let busy = false;
async function refresh() {
  if (busy) return; busy = true;
  try {
    const [desk, payments, mails, requests, gatewayRequests, refundFiles, sent, chaos] = await Promise.all([
      orEmpty(api("GET", "/agents/RefundDesk/acme/tasks")),
      orEmpty(api("GET", "/agents/Payments/acme-pay/tasks")),
      orEmpty(api("GET", "/agents/Mailer/support/tasks")),
      orEmpty(api("GET", "/requests?status=all")),
      orEmpty(api("GET", "/workspace/list/agents/Payments/acme-pay/gateway/requests/")),
      orEmpty(api("GET", "/workspace/list/agents/Payments/acme-pay/gateway/refunds/")),
      orEmpty(api("GET", "/workspace/list/agents/Mailer/support/sent/")),
      orEmpty(api("GET", "/workspace/list/agents/Payments/acme-pay/chaos/")),
    ]);
    const orders = desk.filter((t) => t.task === "RequestRefund").sort((a, b) => b.seq - a.seq);
    $("orders").replaceChildren(...(orders.length ? orders.map((t) => renderOrder(t, { payments, mails }, requests, chaos.includes(t.input && t.input.orderId))) : [h("p", { class: "empty" }, "No requests yet. Pick a preset and send one.")]));
    renderRequests(requests);
    await renderLedger(gatewayRequests, refundFiles, sent);
    $("form-error").textContent = "";
  } catch (e) {
    $("form-error").textContent = e.message;
  } finally { busy = false; }
}
refresh();
setInterval(refresh, 1000);
</script>
</body>
</html>
`;
