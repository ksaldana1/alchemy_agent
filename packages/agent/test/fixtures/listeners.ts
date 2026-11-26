// Listener test fixtures (M6).
import * as Listen from "../../src/Listen.ts";
import { Echo } from "./agents.ts";
import { Greeting } from "./workflows.ts";

export const listeners = [
  // A run per file: the run's key is the file's name, so a duplicate event starts nothing new.
  Listen.files("shared/inbox/", { suffix: ".txt" }).start(Greeting, (file) => ({ key: file.basename })),
  // A task per file, keyed by its path (EchoKeyed's primaryKey).
  Listen.files("shared/tasks/").send(Echo, "listened", "EchoKeyed", (file) => ({ id: file.path, text: file.basename })),
  // A mapping that always throws: its events are dead-lettered.
  Listen.files("shared/poison/", { name: "poison" }).start(Greeting, () => {
    throw new Error("poison");
  }),
  // A task per fire: no primaryKey, so the task id is listener/tick/<scheduledTime>.
  Listen.cron("0 0 1 1 *", { name: "tick" }).send(Echo, "ticker", "EchoText", (fire) => ({ text: `tick ${fire.scheduledTime}` })),
];
