# Animus

Animus is a platform for building applications based on a simple key-value data model.

The main idea is based on the fact that hierarchical structures can be flattened into a set of key-value pairs. For example, we can convert JSON into properties using `yq -o properties`:

```json
{
  "name": "John",
  "measurements": {
    "weight": 80,
    "height": 180
  }
}
```

```properties
name = John
measurements.weight = 80
measurements.height = 180
```

We could do the same for any hierarchical input including xml, yaml or even md formats.

Animus provides a storage where those key-values could be stored and from where they could be read. It also aims to provide
such aspects as access control, audit and versioning.

## Architecture

I want to keep this project independent of specific datasources, schemas or protocols. It's a mediator between all of them, not the best or the only one key-value storage.
