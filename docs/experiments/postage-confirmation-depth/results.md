# Results: the postage confirmation depth setting

Issue: [#545](https://github.com/crtahlin/wasp/issues/545). Spec:
[spec.md](spec.md). Merged as `870dbd54` (#553), tag
`exp-postage-confirmation-depth`.

## Outcome

**The setting works as specified.** With `postage-confirmation-depth: 12` the
node's synced postage block stayed 13 to 16 blocks behind the chain head,
against 5 to 8 with the default of 4. **Whether the default should change is
still open:** that needs the reorg depth of Gnosis Chain after its move to the
Ethereum Economic Zone, and #545 stays open for it.

## Results

**Table: postage listener lag behind the chain head (`chainTip` minus `block` in `/chainstate`), bench-1, 2026-09-28**

| Setting | Sample 1 | Sample 2 | Sample 3 | Expected |
|---|---|---|---|---|
| default (4) | 7 | 8 | 5 | 4 to 8: the depth, plus up to 4 for the step of 5 |
| 12 | 16 | 13 | 15 | 12 to 16 |

Samples were taken a minute apart. The node restarted cleanly with the setting
and after it was removed (`NRestarts` 0).

## Not measured on a node

The refusal of 0 and of values above 64 at start is covered by unit tests. The
reorg measurement on the EEZ testnet is the open part of #545.

Generated with help of AI.
