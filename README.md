# Thread pool for Rust

A library for executing tasks on reusable threads.

[Documentation](https://docs.rs/thread-pool)

## Usage

First add this to your `Cargo.toml`

```toml
[dependencies]
thread-pool = "0.1"
```

Next, add this to your crate:

```rust
extern crate thread_pool;

use thread_pool::ThreadPool;
```

## License

`thread-pool` is primarily distributed under the terms of both the MIT
license and the Apache License (Version 2.0), with portions covered by various
BSD-like licenses.

See LICENSE-APACHE, and LICENSE-MIT for details.

The library is also inspired from parts of [JSR-166](http://g.oswego.edu/dl/concurrency-interest/) which is released to the [public domain](https://creativecommons.org/licenses/publicdomain/).
