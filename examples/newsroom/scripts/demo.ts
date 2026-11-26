// The newsroom against a deployed stage (M5's deployed check). From the repo root:
//   bun run newsroom:demo <url> publish [name] [signOffWithin]   upload an article, start the run, wait for the reviews
//   bun run newsroom:demo <url> submit [name]                    M6: upload only; the listener starts the run. Then replay the event
//   bun run newsroom:demo <url> poison                           M6: put garbage on the listeners' queue; it's dead-lettered
//   bun run newsroom:demo <url> approve <executionId> [yes|no]  sign off (or not) as a person, answer again (a no-op), wait for the outcome
//   bun run newsroom:demo <url> stale <executionId>             M7: change the reviewed file, then try to sign off the old revision
//   bun run newsroom:demo <url> requests                        the pending human requests
//   bun run newsroom:demo <url> status <executionId>
// The token comes from AGENT_API_TOKEN (examples/newsroom/.env).
const [url, step, ...rest] = process.argv.slice(2);
const token = process.env.AGENT_API_TOKEN;
if (!url || !step || !token) {
  console.error("usage: demo.ts <url> publish|approve|status …  (needs AGENT_API_TOKEN)");
  process.exit(1);
}

const ARTICLE = `City council aproves new bike lanes

The city council voted 7 to 2 on tuesday to aprove a network of protected bike lanes downtown. The
project, which will cost about $4.2 million, is expected to be finished by the end of next year.
Supporters says the lanes will make cycling safer; opponents worry about the loss of 120 parking spaces.`;

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
const api = async (method: string, path: string, body?: unknown, raw = false): Promise<any> => {
  const res = await fetch(`${url}${path}`, {
    method,
    headers: { authorization: `Bearer ${token}` },
    ...(body === undefined ? {} : { body: raw ? String(body) : JSON.stringify(body) }),
  });
  const text = await res.text();
  try {
    return JSON.parse(text);
  } catch {
    return text;
  }
};
const status = (id: string) => api("GET", `/workflows/PublishArticle/runs/${id}`);
/** The task the run's n-th agent call created (calls: File, Edit, Share, junior Review, senior Review, sign-off, Share, Publish). */
const callTask = (type: string, name: string, id: string, n: number) =>
  api("GET", `/agents/${type}/${name}/tasks/${encodeURIComponent(`wf/PublishArticle/${id}/c${n}`)}`);
const settled = (t: any) => t?.status === "completed" || t?.status === "failed";

if (step === "publish") {
  const name = rest[0] ?? `bike-lanes-${Date.now().toString(36)}`;
  const path = `shared/submissions/${name}.txt`;
  await api("PUT", `/workspace/files/${path}`, ARTICLE, true);
  const { executionId } = await api("POST", "/workflows/PublishArticle/runs", {
    payload: { path, name, ...(rest[1] ? { signOffWithin: rest[1] } : {}) },
  });
  console.log(`run ${executionId} for ${path}`);
  const t0 = Date.now();
  let senior: any;
  for (let i = 0; i < 240 && !settled(senior); i++) {
    await sleep(1000);
    senior = await callTask("Reviewer", "senior", executionId, 4);
  }
  const junior = await callTask("Reviewer", "junior", executionId, 3);
  const edit = await callTask("Editor", "desk", executionId, 1);
  console.log(`reviews in after ${((Date.now() - t0) / 1000).toFixed(1)} s`);
  console.log(`  edit:   ${edit.status}, attempts ${edit.attempts}`);
  console.log(`  junior: ${junior.status}, ${JSON.stringify(junior.error?.value ?? junior.result)}`);
  console.log(`  senior: ${senior.status}, ${JSON.stringify(senior.result ?? senior.error)}`);
  console.log(`  run:    ${JSON.stringify(await status(executionId))}`);
  console.log(`\nsign off: bun run newsroom:demo ${url} approve ${executionId}`);
} else if (step === "approve" || step === "stale") {
  const id = rest[0]!;
  // The run's sign-off is a human request (its causation is the run).
  let request: any;
  for (let i = 0; i < 120 && request === undefined; i++) {
    request = ((await api("GET", "/requests")) as any[]).find((r) => r.causation === `workflow/PublishArticle/${id}`);
    if (request === undefined) await sleep(1000);
  }
  if (request === undefined) throw new Error(`no pending request from run ${id}`);
  console.log(`request ${request.id}: ${request.prompt}\n  about ${request.artifact.path} @ ${request.artifact.revision}`);
  const respond = (by: string, approved: boolean) =>
    api("POST", `/requests/${encodeURIComponent(request.id)}/respond`, { response: { approved }, by, revision: request.artifact.revision });
  if (step === "stale") {
    await api("PUT", `/workspace/files/${request.artifact.path}`, "Someone rewrote this after the review.", true);
    console.log("changed the file after the review; signing off the reviewed revision:");
    console.log("  ", JSON.stringify(await respond("editor-in-chief", true)));
    console.log("request now:", (await api("GET", `/requests/${encodeURIComponent(request.id)}`)).status);
  } else {
    const approved = (rest[1] ?? "yes") === "yes";
    const first = await respond("editor-in-chief", approved);
    console.log(`answered: ${first.status} by ${first.answer?.by}`);
    const second = await respond("someone-else", !approved);
    console.log(`answered again: ${second.error} (the request stays ${second.request?.status}, by ${second.request?.answer?.by})`);
    let s: any;
    for (let i = 0; i < 60; i++) {
      s = await status(id);
      if (s._tag !== "Running") break;
      await sleep(1000);
    }
    console.log("run:", JSON.stringify(s));
    if (s._tag === "Completed") console.log(`\n${await api("GET", `/workspace/files/${s.result.path}`)}`);
    const publish = await callTask("Intake", "intake", id, 7);
    if (publish?.status) console.log(`publish task: ${publish.status}, attempts ${publish.attempts}`);
  }
} else if (step === "requests") {
  console.log(JSON.stringify(await api("GET", "/requests"), null, 2));
} else if (step === "submit") {
  // M6: the upload alone starts the run (the listener), and a replay of its event starts nothing new.
  const name = rest[0] ?? `bike-lanes-${Date.now().toString(36)}`;
  const path = `shared/submissions/${name}.txt`;
  const ref = await api("PUT", `/workspace/files/${path}`, ARTICLE, true);
  const eventId = `${path}@${ref.revision}`;
  console.log(`uploaded ${path} (revision ${ref.revision})`);
  const deliveries = async () => ((await api("GET", "/listeners/log")) as any[]).filter((e) => e.eventId === eventId && e.kind === "delivered");
  const t0 = Date.now();
  let found: any[] = [];
  for (let i = 0; i < 180 && found.length === 0; i++) {
    await sleep(1000);
    found = await deliveries();
  }
  console.log(`the listener started ${found[0]?.detail} after ${((Date.now() - t0) / 1000).toFixed(1)} s`);
  // Replay the same R2 event by hand into the queue.
  await api("POST", "/listeners/events", {
    message: { action: "PutObject", bucket: "replay", object: { key: path, size: ref.size, eTag: ref.revision }, eventTime: new Date().toISOString() },
  });
  for (let i = 0; i < 60 && found.length < 2; i++) {
    await sleep(1000);
    found = await deliveries();
  }
  const runs = new Set(found.map((e) => e.detail));
  console.log(`${found.length} deliveries of that event → ${runs.size} run(s): ${[...runs].join(", ")}`);
  const id = found[0]?.detail.split("/").at(-1);
  if (id) console.log(`run status: ${JSON.stringify(await status(id))}
follow it: bun run newsroom:demo ${url} status ${id}`);
} else if (step === "poison") {
  const marker = `poison-${Date.now().toString(36)}`;
  await api("POST", "/listeners/events", { message: { garbage: marker } });
  console.log(`sent ${marker}; waiting for the dead letter (3 deliveries, 2 s apart, then the dead-letter queue)…`);
  for (let i = 0; i < 120; i++) {
    await sleep(2000);
    const entries = ((await api("GET", "/listeners/log")) as any[]).filter((e) => e.message?.garbage === marker);
    const dead = entries.find((e) => e.kind === "dead-letter");
    if (dead) {
      console.log(entries.map((e) => `  ${e.kind} (attempts ${e.attempts}): ${e.detail}`).join("\n"));
      break;
    }
  }
} else if (step === "status") {
  console.log(JSON.stringify(await status(rest[0]!), null, 2));
}

export {};
