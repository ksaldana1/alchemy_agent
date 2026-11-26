// The newsroom against a deployed stage (M5's deployed check). From the repo root:
//   bun run newsroom:demo <url> publish [name] [signOffWithin]   upload an article, start the run, wait for the reviews
//   bun run newsroom:demo <url> approve <executionId> [yes|no]  sign off (or not), then wait for the outcome
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
/** The task the run's n-th agent call created (calls: File, Edit, Share, junior Review, senior Review, Share, Publish). */
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
} else if (step === "approve") {
  const id = rest[0]!;
  const approved = (rest[1] ?? "yes") === "yes";
  console.log("before:", JSON.stringify(await status(id)));
  await api("POST", `/workflows/PublishArticle/runs/${id}/signals/sign-off`, { value: { approved, by: "editor-in-chief" } });
  let s: any;
  for (let i = 0; i < 60; i++) {
    s = await status(id);
    if (s._tag !== "Running") break;
    await sleep(1000);
  }
  console.log("after: ", JSON.stringify(s));
  if (s._tag === "Completed") console.log(`\n${await api("GET", `/workspace/files/${s.result.path}`)}`);
  const publish = await callTask("Intake", "intake", id, 6);
  if (publish?.status) console.log(`publish task: ${publish.status}, attempts ${publish.attempts}`);
} else if (step === "status") {
  console.log(JSON.stringify(await status(rest[0]!), null, 2));
}

export {};
