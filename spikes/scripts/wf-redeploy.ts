// Spike 0.4 / W6: a redeploy while a run waits. From spikes/:
//   bun scripts/wf-redeploy.ts start  <doUrl> <nativeUrl>         starts one run per engine, waits until both wait for approval
//   (redeploy: bun --bun alchemy deploy src/wf/stack.ts --stage dev_$USER --force --yes)
//   bun scripts/wf-redeploy.ts finish <doUrl> <nativeUrl> <key>   approves both, waits for the results, checks the tasks
const [step, doUrl, nativeUrl, keyArg] = process.argv.slice(2);
const token = process.env.AGENT_API_TOKEN;
if (!step || !doUrl || !nativeUrl || !token) {
  console.error("usage: wf-redeploy.ts start|finish <doUrl> <nativeUrl> [key]  (needs AGENT_API_TOKEN)");
  process.exit(1);
}
const engines = { do: { url: doUrl, job: "JobDo", wf: "PipelineDo" }, native: { url: nativeUrl, job: "JobNative", wf: "PipelineNative" } };
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
const json = async (res: Response): Promise<any> => (res.status === 204 ? null : res.json());
const start = (url: string, key: string) =>
  fetch(`${url}/wf/start`, { method: "POST", body: JSON.stringify({ key, ms: 300, approvalTimeoutMs: 0 }) }).then(json) as Promise<{ executionId: string }>;
const status = (url: string, id: string) => fetch(`${url}/wf/${id}/status`).then(json);
const task = (e: (typeof engines)["do"], agent: string, id: string, n: number) =>
  fetch(`${e.url}/agents/${e.job}/${agent}/tasks/${encodeURIComponent(`wf/${e.wf}/${id}/c${n}`)}`, {
    headers: { authorization: `Bearer ${token}` },
  }).then(json);

if (step === "start") {
  const key = `redeploy-${Date.now().toString(36)}`;
  for (const [name, e] of Object.entries(engines)) {
    const { executionId } = await start(e.url, key);
    for (let i = 0; i < 120; i++) {
      const t = await task(e, `x-${key}`, executionId, 3).catch(() => null);
      if (t?.status === "failed") break;
      await sleep(500);
    }
    await sleep(4_000); // past the 2 s cooldown: now waiting for the approval
    console.log(name, executionId, await status(e.url, executionId));
  }
  console.log(`\nredeploy now, then: bun scripts/wf-redeploy.ts finish ${doUrl} ${nativeUrl} ${key}`);
} else {
  const key = keyArg!;
  for (const [name, e] of Object.entries(engines)) {
    const { executionId } = await start(e.url, key); // same key: the same run
    console.log(name, "before approval:", await status(e.url, executionId));
    await fetch(`${e.url}/wf/${executionId}/approve`, { method: "POST", body: JSON.stringify({ ok: true }) });
    let s: any;
    for (let i = 0; i < 120; i++) {
      s = await status(e.url, executionId);
      if (s._tag !== "Running") break;
      await sleep(1_000);
    }
    console.log(name, "after approval:", JSON.stringify(s));
    for (const [job, n] of [["a", 0], ["b", 1], ["c", 2]] as const) {
      const t = await task(e, `${job}-${key}`, executionId, n);
      console.log(`  ${job}: ${t?.status} attempts=${t?.attempts}`);
    }
  }
}

export {};
