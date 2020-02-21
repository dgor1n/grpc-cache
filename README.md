# grpc-cache #

Setup Redis:

```sh
  $ docker run --name some-redis -d redis
```

To connect to the redis use:

```sh
  $ docker exec -it some-redis sh
```

```sh
  # redis-cli
```

For watching the queue:
```#
  # SMEMBERS q
```
