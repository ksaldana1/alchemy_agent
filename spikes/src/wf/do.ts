// Spike 0.4: the pipeline on Alchemy's Durable Object engine.
import { make } from "./pipeline.ts";

const { Worker } = make("do", import.meta.url);
export default Worker;
