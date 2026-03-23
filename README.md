# Replicator

A more efficient way to manage connections using hypercore updates as context.

## Usage

```js
const Replicator = require('replicator')

const store = new Corestore('./storage')
const rep = new Replicator(store)
await rep.ready()

// Do stuff with hypercores

// Check stats
console.log(rep.connections)

await rep.close()
```

## API

#### `const rep = new Replicator(store, opts = {})`

Create a new replicator instance to watch a corestore to establish connections for newly opened cores.

`opts` includes:

```
{
  dht: new DHT(), // A `hyperdht` instance to use. Defaults to creating a new instnace
  seed: undefined, // A seed to use to generate the keyPair for the replicator if provided
  keyPair: store.createKeyPair('replicator'), // The keypair to listen with the `dht`. Defaults to being derived from the corestore if not generated via a `seed` or passed as an option.
}
```

#### `await rep.ready()`

Await the replicator setting up it's DHT server and hook to watch the `store` adding new cores.

#### `rep.server`

The DHT server used to connect and manage discovery key topics.
