// Spike 0.3 experiment driver. Runs against any base URL, so local and deployed runs match:
//   bun spikes/scripts/probe.ts <baseUrl> <experiment...>
//   bun spikes/scripts/probe.ts http://localhost:1337 E1 E2 E4
// Each experiment uses a fresh Probe name and prints that probe's log with times relative to
// the first row. Experiments that span a redeploy (E5, E6) have "-start" and "-check" halves.

const [base, ...experiments] = process.argv.slice(2);
if (!base || experiments.length === 0) {
  console.error("usage: bun spikes/scripts/probe.ts <baseUrl> <experiment...>");
  process.exit(1);
}
const run = process.env.PROBE_RUN ?? Date.now().toString(36).slice(-5);

const call = async (name: string, method: string, ...args: unknown[]) => {
  const res = await fetch(`${base}/probe/${name}/${method}`, {
    method: args.length ? "POST" : "GET",
    body: args.length ? JSON.stringify(args) : undefined,
  });
  const text = await res.text();
  try {
    return JSON.parse(text);
  } catch {
    throw new Error(`${method} → ${res.status}: ${text.slice(0, 300)}`);
  }
};

type Row = { seq: number; at: number; event: string; id: string; attempt: number; detail: string };
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

const show = async (name: string, label = name) => {
  const rows = (await call(name, "log")) as Row[];
  const t0 = rows[0]?.at ?? 0;
  console.log(`--- ${label} (${rows.length} rows)`);
  for (const r of rows) {
    const t = `+${((r.at - t0) / 1000).toFixed(2)}s`.padStart(9);
    console.log(`${t}  ${r.event.padEnd(14)} ${r.id.padEnd(10)} #${r.attempt}  ${r.detail}`);
  }
  return rows;
};

/** Poll the log until `done(rows)` or the timeout; then print it. */
const waitFor = async (name: string, done: (rows: Row[]) => boolean, timeoutMs: number) => {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    const rows = (await call(name, "log")) as Row[];
    if (done(rows)) break;
    await sleep(500);
  }
  return show(name);
};
const count = (rows: Row[], event: string, id?: string) =>
  rows.filter((r) => r.event === event && (id === undefined || r.id === id)).length;

const experimentsById: Record<string, () => Promise<void>> = {
  // E1: fires after ~2 s.
  async E1() {
    const p = `e1-${run}`;
    await call(p, "schedule", { afterMs: 2000, payload: { id: "a" } });
    await waitFor(p, (r) => count(r, "end") >= 1, 30_000);
  },
  // E1b: same, after the DO has sat idle long enough to be evicted (~70 s before scheduling).
  async E1idle() {
    const p = `e1i-${run}`;
    await call(p, "schedule", { afterMs: 65_000, payload: { id: "idle" } });
    await waitFor(p, (r) => count(r, "end") >= 1, 120_000);
  },
  // E2: schedule inside a transaction that fails; the callback must not fire.
  async E2() {
    const p = `e2-${run}`;
    console.log("scheduleInTxThenFail →", JSON.stringify(await call(p, "scheduleInTxThenFail", "tx")));
    console.log("status →", JSON.stringify(await call(p, "status", "tx")));
    await sleep(6000);
    await show(p);
    console.log("status after 6 s →", JSON.stringify(await call(p, "status", "tx")));
  },
  // E3: fails 3 times with a 2 s retry delay, then succeeds.
  async E3() {
    const p = `e3-${run}`;
    await call(p, "schedule", { payload: { id: "f3", failTimes: 3 } });
    await waitFor(p, (r) => count(r, "end") >= 1, 60_000);
  },
  // E3b: never succeeds; maxAttempts 3, delay 1 s. Does it give up, or park?
  async E3park() {
    const p = `e3p-${run}`;
    await call(p, "schedule", { callback: "flaky", payload: { id: "never", failTimes: 1000 } });
    await waitFor(p, (r) => count(r, "fail") >= 3, 30_000);
    await sleep(8000);
    await show(p, `${p} after 8 s more`);
    console.log("status →", JSON.stringify(await call(p, "status", "never", "flaky")));
  },
  // E4: schedule the same id twice with different payloads; only the second runs.
  async E4() {
    const p = `e4-${run}`;
    await call(p, "schedule", { afterMs: 2000, payload: { id: "same", note: "first" } });
    await call(p, "schedule", { afterMs: 2000, payload: { id: "same", note: "second" } });
    await sleep(8000);
    await show(p);
  },
  // E5 / E6: schedule 60 s out; redeploy in between; check afterwards.
  async E5start() {
    const p = `e5-${process.env.PROBE_RUN ?? "x"}`;
    await call(p, "reset");
    await call(p, "schedule", { afterMs: 60_000, payload: { id: "late", note: "before redeploy" } });
    await show(p);
  },
  async E5check() {
    await waitFor(`e5-${process.env.PROBE_RUN ?? "x"}`, (r) => count(r, "end") >= 1, 120_000);
  },
  async E6start() {
    const p = `e6-${process.env.PROBE_RUN ?? "x"}`;
    await call(p, "reset");
    await call(p, "schedule", { callback: "orphan", afterMs: 45_000, payload: { id: "orph" } });
    await show(p);
  },
  async E6check() {
    const p = `e6-${process.env.PROBE_RUN ?? "x"}`;
    await show(p);
    console.log("status (via work handle) →", JSON.stringify(await call(p, "status", "orph", "orphan")));
  },
  // E7: 100 callbacks with after 0 in one transaction.
  async E7() {
    const p = `e7-${run}`;
    await call(p, "scheduleMany", 100);
    const rows = await waitFor(p, (r) => count(r, "end") >= 100, 60_000);
    const starts = rows.filter((r) => r.event === "start");
    const ids = starts.map((r) => r.id);
    const sorted = [...ids].sort();
    const overlapping = rows.some(
      (r, i) => r.event === "start" && rows[i + 1]?.event === "start",
    );
    const first = rows.find((r) => r.event === "scheduled-many")?.at ?? 0;
    const last = rows.filter((r) => r.event === "end").at(-1)?.at ?? 0;
    console.log(
      `E7 summary: ${count(rows, "end")} ended, in id order: ${JSON.stringify(ids) === JSON.stringify(sorted)}, ` +
        `two starts in a row (interleaved): ${overlapping}, total ${(last - first) / 1000}s`,
    );
  },
  // E8: a callback sleeps 5 s (retry delay 2 s) while RPC calls hit the DO.
  async E8() {
    const p = `e8-${run}`;
    await call(p, "schedule", { payload: { id: "slow", sleepMs: 5000 } });
    for (let i = 0; i < 8; i++) {
      await sleep(800);
      await call(p, "ping", `rpc${i}`);
    }
    await waitFor(p, (r) => count(r, "end") >= 1, 20_000);
    await sleep(4000);
    await show(p, `${p} 4 s later (a second "start" means the job ran twice)`);
  },
  // E9: a chain of 10 self-reschedules, without and with progress.
  async E9() {
    const p = `e9-${run}`;
    await call(p, "schedule", { payload: { id: "chain", chain: 10 } });
    await waitFor(p, (r) => count(r, "end") >= 11, 90_000);
    console.log("status →", JSON.stringify(await call(p, "status", "chain")));
  },
  async E9progress() {
    const p = `e9p-${run}`;
    await call(p, "schedule", { payload: { id: "chainp", chain: 10, withProgress: true } });
    await waitFor(p, (r) => count(r, "end") >= 11, 60_000);
  },
  // E10: CPU-heavy callbacks. Iteration counts come from E10cal on local workerd.
  async E10cal() {
    const p = `e10cal-${run}`;
    await call(p, "schedule", { payload: { id: "cal", cpuIters: 200_000_000 } });
    await waitFor(p, (r) => count(r, "end") >= 1, 60_000);
  },
  async E10() {
    const perMs = Number(process.env.ITERS_PER_MS ?? 0);
    if (!perMs) throw new Error("set ITERS_PER_MS from E10cal");
    for (const ms of (process.env.CPU_MS ?? "1000,10000,25000,40000").split(",").map(Number)) {
      const p = `e10-${ms}-${run}`;
      await call(p, "schedule", { payload: { id: `cpu${ms}`, cpuIters: Math.round(ms * perMs) } });
      // Polling a DO that is busy would queue behind the loop, so wait first.
      await sleep(ms + 15_000);
      await show(p).catch((e) => console.log(`log failed: ${e}`));
      console.log("jobs →", JSON.stringify(await call(p, "jobs").catch((e) => String(e))));
      await call(p, "cancel", `cpu${ms}`).catch(() => {});
    }
  },
  // E11: a tagged error vs a defect; both should retry.
  async E11() {
    const p = `e11-${run}`;
    await call(p, "schedule", { payload: { id: "tagged", failTimes: 1, failMode: "tagged" } });
    await call(p, "schedule", { payload: { id: "die", failTimes: 1, failMode: "die" } });
    await waitFor(p, (r) => count(r, "end") >= 2, 30_000);
  },
  // E12: non-JSON payloads.
  async E12() {
    const p = `e12-${run}`;
    console.log("Date →", JSON.stringify(await call(p, "scheduleBad", "date")));
    console.log("undefined →", JSON.stringify(await call(p, "scheduleBad", "undefined")));
  },
  // Drain loop: 3 enqueues of 4 tasks each (one duplicate id), batch size 3.
  async DRAIN() {
    const p = `drain-${run}`;
    await Promise.all([
      call(p, "enqueue", ["t01", "t02", "t03", "t04"]),
      call(p, "enqueue", ["t05", "t06", "t07", "t08"]),
      call(p, "enqueue", ["t09", "t10", "t11", "t04"]),
    ]);
    const start = Date.now();
    let inbox: Array<{ task_id: string; status: string; runs: number }> = [];
    while (Date.now() - start < 30_000) {
      inbox = await call(p, "inbox");
      if (inbox.every((r) => r.status === "done")) break;
      await sleep(300);
    }
    await show(p);
    console.log(
      `DRAIN summary: ${inbox.length} rows, all done: ${inbox.every((r) => r.status === "done")}, ` +
        `runs per row: ${JSON.stringify([...new Set(inbox.map((r) => r.runs))])}`,
    );
  },
};

for (const id of experiments) {
  const fn = experimentsById[id];
  if (!fn) throw new Error(`unknown experiment ${id}`);
  console.log(`\n===== ${id} @ ${base}`);
  await fn();
}

export {};
