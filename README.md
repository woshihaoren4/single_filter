# single_filter

> Filter duplicate keys

## quick start

In a distributed environment, filter a single key.

```rust
    let strategy =
        BloomExpandStrategy::build_from_redis("biz02", "redis://:root@127.0.0.1/")
            .unwrap();
    let pool = FiltersPool::from(strategy);

    let exists = pool.contain("user001","key001".into()).await.unwrap();
    assert_eq!(exists,false);
    pool.insert("user001","key001".into()).await.unwrap();
    let exists = pool.contain("user001","key001".into()).await.unwrap();
    assert_eq!(exists,true);
```
## batch

In a recommendation system, you can call the system in batches, but this method has concurrency problems for the same group.

```rust
pool.batch_contain(group, keys).await.unwrap();

pool.batch_insert(group, keys).await.unwrap();
```

## expansion strategy

- set_strategy_fixed : Expand to a fixed size
- set_strategy_ladder : Use fixed policies to expand capacity
- set_strategy_function : User-defined expansion mode

## other

grpc service and docker image need to be improved