# A setting for how many blocks the postage listener treats as final

Issue: [#545](https://github.com/crtahlin/wasp/issues/545). Split out of
[#540](https://github.com/crtahlin/wasp/issues/540).
Type: fix.

## Problem

The postage listener only applies events from blocks at least 4 behind the chain
head (`pkg/postage/listener/listener.go`, `tailSize = 4`). A block that deep is
treated as final. Batch state never moves backwards (`batchservice.go` refuses a
lower block), so an event applied from a block that is later rolled back stays
applied.

**Table: what 4 blocks mean, now and after Gnosis Chain's move to the Ethereum Economic Zone**

| | Block time | 4 blocks | Reorgs |
|---|---|---|---|
| Gnosis Chain today | 5 s | 20 s | rare in practice |
| After GIP-153 (targeted December 2026 or January 2027) | 2 s | 8 s | the chain is "forced to reorg" with Ethereum, "around 5-10 times a day"; one Ethereum slot spans 6 rollup blocks |

A batch purchase, top-up or dilution from a rolled-back block could be applied by
some nodes and not by others. Those nodes then disagree on which stamps are
valid.

## Deviation from rule 8, recorded

Rule 8 says a tuning constant becomes a setting only after a measurement shows
its value matters. That measurement is the rollup's real reorg depth and
frequency, which is not possible yet. The operator decided on 2026-09-28 to add
the setting ahead of it, so that nodes can be adjusted without a new release
when the switch comes ("we can always change later"). **The default stays 4.**
The measurement below still decides whether the default changes.

## Design

**New setting: `postage-confirmation-depth`**, the number of blocks behind the
chain head that postage events must be before they are applied.

- Default 4, which is today's value, so nothing changes unless it is set.
- Values below 1 are refused at start, with an error naming the setting. A
  depth of 0 would apply events from the head itself, which is never safe. This
  follows #492: stop on a bad value instead of looping on it.
- **Values above 64 are refused too** (added in review of #553). While the
  listener waits for the chain to move the depth past what it has synced, it
  makes no progress, and after the 10-minute postage stall limit the node stops
  and is restarted, over and over. 64 blocks is 320 s at 5 s blocks and 128 s
  at 2 s blocks, so even a raise from 4 to 64 on a synced node stays under the
  limit.
- It applies to the live listener and to the listener behind the snapshot
  loader.

**What raising it costs:**
- A stamp purchase, top-up or dilution is seen that many blocks later: 5 s per
  block today, 2 s after the switch.
- A newly bought batch becomes usable later by the same amount. The node's
  "usable after 10 blocks" rule counts from the block it has synced.
- **Other nodes are not affected.** It changes only when this node sees events.

**What lowering it costs:**
- This node applies events from blocks that may still be rolled back, and keeps
  them.
- **Its peers pay for that:** it may accept chunks stamped with a batch the rest
  of the network never saw, or reject chunks stamped with one it missed.

**Why not follow the chain's `safe` or `finalized` block tag instead:** whether
the rollup serves those tags, and what they lag by, is not known yet. That is
part of the measurement below. If they are served and useful, following them
can replace or extend this setting later.

## Not changed

- `blockThreshold` (10 blocks before a batch is usable) and the listener's step
  of 5 blocks.
- How the listener behaves after it sees a lower block.

## Protocol impact

None. The setting changes only when this node applies chain events. Nothing on
the wire changes.

## Tests

- The listener stays exactly the configured number of blocks behind the head:
  4 by default, 12 when set.
- A value of 0 is refused at start.
- The snapshot loader passes the setting through.

Mutations: ignore the setting (use 4); accept 0.

## Measurement

1. **Now, on the bench:** a node with `postage-confirmation-depth: 12` reports
   a synced block in `/chainstate` that stays 12 to 16 blocks behind the head.
   That is the depth plus the step of 5. Three samples, against the default's
   4 to 8.
2. **When the EEZ testnet exists:** record reorg depth and frequency over at
   least 24 hours. Then decide the default and whether to follow the `safe` tag
   (#545 stays open for this).

## Rollout and rollback

A new setting with today's value as its default. Rollback is reverting the
merge; a node that set it then fails at start on an unknown option, so remove it
from the configuration first.

## Upstream portability

`tailSize` is unmodified upstream code. The setting is this fork's own.

## Files

- `cmd/bee/cmd/cmd.go`, `start.go` (the option, validation)
- `pkg/node/node.go` (wiring)
- `pkg/postage/listener/listener.go`, `pkg/postage/snapshot/factory.go`
- tests next to each
- `docs/config-reference.yaml`, `docs/DIFFERENCES.md`,
  `docs/experiments/INDEX.md`

Generated with help of AI.
