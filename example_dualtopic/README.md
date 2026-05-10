# Dualtopic scenario

This example simulates peer clusters over a shared counter autobase view.

Each fictional "user" has 4 devices expressed as independent peers.
Each peer pins a `group` topic as an analogy to a user's private topic.

Each peer autoswarms on the common global topic.

On long interval some peers bump the counter value.

The new value propagates quickly via the group topic and signals all peers in group to actively rejoin the common topic to seed new value.

GC-parameters and limits were tweaked in this scenario to
speed up and capture the behavior.

## Theory

This is a small-swarm pressure test.

The shared counter is only a signal. It gives every peer the same value
to chase while the discovery graph is forced through a narrow connection
budget.

Each group behaves like a user's device set. The group topic is pinned
and should stay locally useful. The global topic is shared by everyone,
but it is allowed to fall inactive, get evicted, and later be re-entered
when a newer counter value is observed.

The scenario asks whether replication can keep making progress when the
obvious global rendezvous point is not always active. It also makes
connection churn visible: cap repair, rookie eviction and stale-topology
rotation should be separable in the log.

The knobs are intentionally compressed so these behaviours appear in a
short run:

| Option                    | Default | Scenario | Why                                                                                   |
| ------------------------- | ------- | -------- | ------------------------------------------------------------------------------------- |
| `manual`                  | `false` | `true`   | Drive refresh from the scenario clock so GC and discovery can be observed in the log. |
| `slots`                   | `8`     | `1`      | Force competition between the global topic and the dummy topic.                       |
| `minPeers`                | `20`    | `0`      | Allow the simulation to evict below the production floor.                             |
| `maxPeers`                | `40`    | `2`      | Keep each peer below the number of available discovery topics.                        |
| `gcIdleTime`              | `30s`   | `5s`     | Make inactive sessions disappear during a short run.                                  |
| `minActiveTime`           | `30s`   | `5s`     | Keep a selected topic active long enough to observe it before rotation.               |
| `defaultDiscoveryTimeout` | `60s`   | `5s`     | Shorten discovery windows for topics with no fresh bump.                              |
| `updatedDiscoveryTimeout` | `10s`   | `3s`     | Shorten discovery windows after a fresh counter update.                               |
| `nomad`                   | `60s`   | `10s`    | Wanderlust timeout, stale topology escape.                                            |

The global topic may be inactive or evicted.
Group topics remain pinned as local device-set anchors.
Only the shared counter determines whether replication converged.

### Usage

Generate the log:

```bash
bare scenario.js | tee out.ndjson
```

Open `renderer.html` in browser; drag & drop the log.
