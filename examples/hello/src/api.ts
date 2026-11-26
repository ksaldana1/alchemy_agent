import { Api } from "alchemy-agent";
import { Greeter } from "./greeter.ts";

export default Api.worker("HelloApi", { main: import.meta.url, agents: [Greeter] });
