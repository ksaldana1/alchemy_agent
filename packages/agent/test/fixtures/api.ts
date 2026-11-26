import * as Api from "../../src/Api.ts";
import { Echo, Flaky, Relay } from "./agents.ts";

export default Api.worker("AgentTestApi", { main: import.meta.url, agents: [Echo, Flaky, Relay] });
