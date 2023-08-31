# chash

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

This library provides a consistent hashring which simultaneously achieves both uniformity and consistency. It is a direct port of the Go pkg <https://github.com/buraksezer/consistent>.

For detailed information about the concept, you should take a look at the following resources:

- [Consistent Hashing with Bounded Loads on Google Research Blog](https://research.googleblog.com/2017/04/consistent-hashing-with-bounded-loads.html)
- [Improving load balancing with a new consistent-hashing algorithm on Vimeo Engineering Blog](https://medium.com/vimeo-engineering-blog/improving-load-balancing-with-a-new-consistent-hashing-algorithm-9f1bd75709ed)
- [Consistent Hashing with Bounded Loads paper on arXiv](https://arxiv.org/abs/1608.01350)

## Wishlist

- [ ] Add benchmarks
- [ ] Moar tests! Especially property based testing
- [ ] async/await API
- [ ] Pluggable storage w/ implementations: in-memory (possibly w/ DashMap), etcd, redis, and foundationdb
- [ ] Performance Tuning (mostly the obvious stuff... no quest for glory)

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or <http://www.apache.org/licenses/LICENSE-2.0>)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or <http://opensource.org/licenses/MIT>)

at your option.

### Contribution

- Contributions are welcome! 🙏
- Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in the work by you, as defined in the Apache-2.0 license, shall be dual licensed as above, without any additional terms or conditions.
