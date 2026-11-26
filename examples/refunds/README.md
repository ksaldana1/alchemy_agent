# Refund desk

An agent that handles refund requests, while things go wrong on purpose. It shows what the
library is for: work with side effects that must happen **once**, even when the code running it
dies partway through.

```sh
bun run refunds:ui           # a live page at http://localhost:1337: click through it yourself
bun run refunds:demo         # the same story narrated in the terminal, about 30 seconds
```

Both run locally on workerd with a scripted model, so there's no LLM bill. They run from a checkout
of this repository, after `bun install`. They also need a Cloudflare account connected to Alchemy
(see the [Hello example](../../README.md#try-the-hello-example)), because the stack keeps its state
in Cloudflare. Nothing is deployed to Cloudflare. `refunds:demo` tears itself down when it's done;
stop `refunds:ui` with Ctrl-C.

## The agents

- **RefundDesk** asks an LLM whether a request is eligible and writes the customer's emails.
  Refunds over $200 wait for a manager's approval. Then it has Payments issue the refund and has
  Mailer email the customer.
- **Payments** calls the payment gateway, using its task id as the idempotency key.
- **Mailer** calls the email provider.

The gateway and the email provider are simulated (`src/providers.ts`). In a real app they would
be Stripe and Postmark. They keep their records in the workspace, so the demo can count what
actually reached them.

## What the demo does

`demo/platform.ts` runs the same agents with a scripted model and one planted failure, at **the
worst moment to crash**. The gateway refunds the order, then the Payments agent dies before it
records the answer, as an eviction, a CPU limit, or a deploy would. The task is delivered again
with the same id, so the gateway sees the same idempotency key and returns the refund it already
made. The crash is armed per order: the page has a checkbox for it, and the terminal demo arms it
for the first order.

### The page (`refunds:ui`)

Pick a preset or write your own request, then watch each order's steps update live: the model,
the manager's approval, each call to Payments and Mailer with how many times it was delivered,
and the outcome. Approve or decline big refunds in the manager's queue, and press **Send again**
on an order to send a duplicate. The **Outside world** panel counts what reached the gateway and
the email provider. When a crash caused a redelivery, it shows more gateway requests than
refunds, and says why.

The API token is `demo-token`. The page is plain HTML and JavaScript (`demo/page.ts`) that polls
the same HTTP API a script would use.

To share it, deploy the page to the `demo_$USER` stage. Put a long random `AGENT_API_TOKEN` in
`examples/refunds/.env` first. The token is the only lock on the whole API, so don't use
`demo-token`. Then:

```sh
bun run refunds:ui:deploy --yes    # prints the URL
bun run refunds:ui:destroy --yes   # when you're done
```

Share it as `https://<url>/#token=<token>`. The page reads the token from the part after `#`, which
browsers don't send to servers or put in logs. Everyone with the link uses the same desk and sees
every order.

### The terminal (`refunds:demo`)

It sends four requests:

| Order | Amount | What happens |
| --- | --- | --- |
| Pour-over kettle | $34 | Approved by policy, through the planted crash. |
| Espresso machine | $649 | Waits for a manager. The task is `waiting`, and no code runs until the answer comes. |
| Linen throw | $89 | The model finds it outside the policy: no refund, a decline email. |
| The kettle again | | A duplicate. Same order, same task, same answer. |

At the end it reads the providers' ledgers: **3 gateway requests, 2 refunds, 3 emails**. Every
number is checked, so the demo is also the example's test.

## How "once" works

- **Calls between agents** go through the caller's outbox and are numbered by their position in
  the handler. When RefundDesk runs again from the top (after the model answers, after the
  manager answers, after each reply), calls it already made return their recorded results
  instead of being sent again.
- **The LLM turn** is a durable step, so the model is asked once per order.
- **`primaryKey: orderId`** makes a repeated request the same task.
- **The outside world** needs one thing from you: pass the task id as the idempotency key. The
  library guarantees that the id stays the same on every delivery.

## Deploy it for real

`src/platform.ts` uses Workers AI (Llama 3.3 70B) instead of the script, and has no planted
failures. Put `AGENT_API_TOKEN` in `examples/refunds/.env`, then:

```sh
bun run refunds:deploy --yes
curl -X POST "$URL/agents/RefundDesk/acme/tasks" -H "authorization: Bearer $AGENT_API_TOKEN" \
  -H "content-type: application/json" \
  -d '{ "tag": "RequestRefund", "payload": { "orderId": "1", "customer": "a@example.com", "item": "Kettle", "amount": 3400, "reason": "It arrived broken." } }'
bun run refunds:destroy --yes
```
