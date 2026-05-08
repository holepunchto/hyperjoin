# Dualtopic scenario

This example simulates peer clusters over a shared counter autobase view.

Each fictional "user" has 3 devices expressed as independent peers.
Each peer pins a `group` topic as an analogy to a user's private topic.

Each peer autoswarms on the common global topic.

On long interval some peers bump the counter value.

The new value propagates quickly via the group topic and signals all peers in group to actively rejoin the common topic to seed new value.

GC-parameters and limits were tweaked in this scenario to
speed up and capture the behavior.

### Usage

Generate the log:
```bash
bare scenario.js | tee out.ndjson
```

Open `renderer.html` in browser; drag & drop the log.

