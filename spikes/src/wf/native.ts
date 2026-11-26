// Spike 0.4: the pipeline on Alchemy's native Cloudflare Workflows engine.
import { make } from "./pipeline.ts";

const { Worker } = make("native", import.meta.url);
export default Worker;
