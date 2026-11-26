// The refund desk: three agents in one Worker, with its HTTP API. The workspace holds the
// simulated providers' records.
import { Platform } from "alchemy-agent";
import { MailerLive, PaymentsLive, RefundDeskLive } from "./agents.ts";

export default Platform.make("RefundsApi", {
  main: import.meta.url,
  agents: [RefundDeskLive, PaymentsLive, MailerLive],
  workspace: { forceDestroy: true },
});
