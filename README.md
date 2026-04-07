# Replicator

A `hyperswarm` replacement modeled for `corestore` that allows
resource tracking and garbage collection.

## Usage

```js
const Corestore = require('corestore')
const Replicator = require('replicator')

const store = new Corestore('./storage')

const rep = new Replicator(store)
await rep.ready()

const session = store.session()

const discoveryKey = '...'
rep.join(session, discoveryKey, { announce: true })

const core = session.get({ name: 'example' })
await core.ready()

await rep.close()
await store.close()
```

## API

TBD - implements subset of `hyperswarm` API

## License

Apache-2.0
