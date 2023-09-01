/// This pkg provides a consistent hashring with bounded loads. This implementation also adds
/// partitioning logic on top of the original algorithm. For more information about the underlying algorithm,
/// please take a look at <https://research.googleblog.com/2017/04/consistent-hashing-with-bounded-loads.html>.
///
/// This pkg is a port of ((and consistent with) the Go pkg [consistent](https://github.com/buraksezer/consistent)
///
/// # Examples
///
/// ```rust
/// use chash::{HashRingConfig, HashRing};
/// use std::{
///    collections::{hash_map::RandomState},
///    fmt::{Display, Formatter},
///    str::FromStr,
/// };
///
/// #[derive(Hash, PartialEq, Eq, Debug, Clone)]
/// struct HashRingNode {
///    uid: String,
/// }
///
/// impl HashRingNode {
///    fn new(uid: String) -> Self {
///       Self { uid }
///   }
/// }
///
/// impl FromStr for HashRingNode {
///  type Err = ();
///  fn from_str(s: &str) -> Result<Self, Self::Err> {
///    Ok(Self::new(s.to_string()))    
///  }
/// }
///
/// impl Display for HashRingNode {
///   fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
///    write!(f, "{}", self.uid)
///   }
/// }
///
/// let ring_config = HashRingConfig::new(23, 20, 1.25, RandomState::new());
/// let ring = HashRing::with_config(ring_config);
/// let node_count: usize = 8;
///
/// let nodes = (0..node_count)
///   .map(|i| HashRingNode::new(format!("node_{}", i)))
///  .collect::<Vec<_>>();
///
/// ring.add_nodes(nodes).unwrap();
///
/// ```
use std::{
    collections::{hash_map::RandomState, BTreeMap, HashMap, HashSet},
    error::Error,
    fmt::{Display, Write},
    hash::{BuildHasher, Hash, Hasher},
    iter::FromIterator,
    str::FromStr,
    sync::{Arc, Mutex},
};

#[derive(Debug, PartialEq, Eq)]
pub enum HashRingError {
    IllegalArgument(String),
    InvalidValue(String),
    StorageLock(String),
    NotFound(String),
}

impl Display for HashRingError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            HashRingError::IllegalArgument(msg) => {
                write!(f, "[HashRingError] Illegal argument: {msg}")
            }
            HashRingError::InvalidValue(msg) => write!(f, "[HashRingError] Value error: {msg}"),
            HashRingError::StorageLock(msg) => {
                write!(f, "[HashRingError] Storage lock error: {msg}")
            }
            HashRingError::NotFound(msg) => write!(f, "[HashRingError] Not found: {msg}"),
        }
    }
}

impl Error for HashRingError {}
pub struct HashRingConfig<H: BuildHasher = RandomState> {
    /// Keys are distributed among partitions. Prime numbers are good to
    /// distribute keys uniformly. Select a big partition count if you have
    /// too many keys.
    partition_count: u64,
    /// Members are replicated on consistent hash ring. This number means that a member
    /// how many times replicated on the ring.
    replication_factor: u64,
    /// Capacity is used to calculate average node load. See the code, the paper and Google's blog post to learn about it.
    load_factor: f64,
    /// Hasher used to hash keys, partitions and nodes.
    build_hasher: H,
}

impl Default for HashRingConfig {
    fn default() -> Self {
        HashRingConfig {
            partition_count: 71,
            replication_factor: 20,
            load_factor: 1.25,
            build_hasher: RandomState::new(),
        }
    }
}

impl<H: BuildHasher> HashRingConfig<H> {
    pub fn new(
        partition_count: u64,
        replication_factor: u64,
        load_factor: f64,
        build_hasher: H,
    ) -> Self {
        HashRingConfig {
            partition_count,
            replication_factor,
            load_factor,
            build_hasher,
        }
    }

    pub fn with_build_hasher(build_hasher: H) -> Self {
        HashRingConfig {
            partition_count: 71,
            replication_factor: 20,
            load_factor: 1.25,
            build_hasher,
        }
    }

    pub fn partition_count(&self) -> u64 {
        self.partition_count
    }

    pub fn replication_factor(&self) -> u64 {
        self.replication_factor
    }

    pub fn load_factor(&self) -> f64 {
        self.load_factor
    }

    pub fn get_hasher(&self) -> <H as BuildHasher>::Hasher {
        self.build_hasher.build_hasher()
    }
}

#[derive(Default)]
struct HashRingStorage {
    /// The hash ring.
    ring: BTreeMap<u64, Vec<u8>>,
    /// The members of the ring.
    members: HashSet<Vec<u8>>,
    /// The partitions of the ring.
    partitions: HashMap<u64, Vec<u8>>,
    /// The loads of each member.
    loads: HashMap<Vec<u8>, f64>,
}

impl HashRingStorage {
    fn average_load(&self, partition_count: u64, load: f64) -> f64 {
        if self.members.is_empty() {
            return 0.0;
        }

        ((partition_count / self.members.len() as u64) as f64 * load).ceil()
    }

    fn has_node(&self, node: Vec<u8>) -> bool {
        self.members.contains(&node)
    }

    fn nodes_count(&self) -> usize {
        self.members.len()
    }

    fn list_nodes(&self) -> Vec<Vec<u8>> {
        self.members.iter().cloned().collect::<Vec<_>>()
    }

    /// Adds nodes to both the hashring and the list of members.
    fn add_nodes(&mut self, nodes: Vec<Vec<u8>>, vnodes: Vec<(u64, Vec<u8>)>) {
        self.members.extend(nodes.iter().cloned());
        self.ring
            .extend(vnodes.iter().map(|(key, value)| (*key, value.clone())));
    }

    /// Removes nodes from the both the hashring and the list of members.
    fn remove_nodes(&mut self, nodes: Vec<Vec<u8>>, vnode_keys: Vec<u64>) {
        let removed_nodes: HashSet<Vec<u8>> = HashSet::from_iter(nodes);
        self.members
            .retain(|member| !removed_nodes.contains(member));

        let removed_vnodes: HashSet<u64> = HashSet::from_iter(vnode_keys);
        self.ring.retain(|key, _| !removed_vnodes.contains(key));

        if self.members.is_empty() {
            self.partitions.clear();
        }
    }

    /// Returns the load distribution of across the nodes on the hashring.
    fn load_distribution(&self) -> HashMap<Vec<u8>, f64> {
        self.loads.clone()
    }

    /// Returns the node that owns the partition.
    fn get_node_for_partition(&self, partition_id: u64) -> Option<Vec<u8>> {
        self.partitions.get(&partition_id).cloned()
    }

    /// Distributes the partitions across the nodes on the hashring.
    fn distribute_partitions(
        &mut self,
        partitions_by_hash: BTreeMap<u64, u64>,
        node_max_capacity: f64,
    ) {
        self.loads.clear();
        self.partitions.clear();

        let ring_size = self.ring.len() as u64;
        let mut successful_placement_count = 0;

        for (partition_hash, partition_id) in partitions_by_hash {
            let mut count = 0;
            let mut nodes = self.ring.range(partition_hash..);

            loop {
                count += 1;

                if count >= self.ring.len() {
                    let error_message = format!(
                        r#"
                            ya done mess'd up!
                            the ring ran out of space while attempting to assign a partition to a node
                            context(ring_size={ring_size}, partition_placement_iteration_count={count}, partition_id={partition_id}, node_max_capacity={node_max_capacity}, successful_placements={successful_placement_count})
                        "#,
                    );

                    panic!("{error_message}");
                }

                if let Some((_, member)) = nodes.next() {
                    let load = self.loads.entry(member.clone()).or_insert(0.0);

                    if (*load + 1.00) <= node_max_capacity {
                        *load += 1.0;
                        self.partitions.insert(partition_id, member.clone());
                        successful_placement_count += 1;
                        break;
                    }
                } else {
                    nodes = self.ring.range(..partition_hash);
                }
            }
        }
    }

    /// Returns the closest N nodes that can host a given partition.
    fn get_closest_nodes_for_partition(
        &self,
        members_by_hash: BTreeMap<u64, Vec<u8>>,
        partition_id: u64,
        n: usize,
    ) -> Result<Vec<Vec<u8>>, HashRingError> {
        if n > self.members.len() {
            return Err(HashRingError::IllegalArgument(format!(
                "cannot get {} nodes for partition; there are only {} nodes in the ring",
                n,
                self.members.len()
            )));
        }

        let mut nodes = Vec::with_capacity(n);

        match self.get_node_for_partition(partition_id) {
            Some(partition_owner) => {
                if let Some(mut partition_owner_position) = members_by_hash
                    .iter()
                    .position(|(_, member)| member == &partition_owner)
                {
                    let member_hash_keys = members_by_hash.keys().copied().collect::<Vec<_>>();

                    // This check is kind of silly, but still...
                    if partition_owner_position >= members_by_hash.len() {
                        partition_owner_position = 0;
                    }

                    while nodes.len() < n {
                        let member_hash = member_hash_keys
                            .get(partition_owner_position)
                            .unwrap_or_else(|| {
                                unreachable!(
                                    "Member hash not found at position {}",
                                    partition_owner_position
                                )
                            });

                        let member = members_by_hash.get(member_hash).unwrap_or_else(|| {
                            unreachable!("Member not found for hash {}", member_hash)
                        });

                        nodes.push(member.clone());

                        partition_owner_position += 1;
                        partition_owner_position %= members_by_hash.len();
                    }

                    Ok(nodes)
                } else {
                    Err(HashRingError::InvalidValue(format!(
                            "partition({partition_id}) does not have a corresponding node on the ring. specifically, no corresponding node was found from the members_by_hash argument"
                        )))
                }
            }
            None => Err(HashRingError::IllegalArgument(format!(
                "partition({partition_id}) does not have a corresponding node on the ring"
            ))),
        }
    }
}

/// A consistent hash ring with virtual nodes.
pub struct HashRing<H: BuildHasher = RandomState> {
    storage: Arc<Mutex<HashRingStorage>>,
    config: Arc<HashRingConfig<H>>,
}

impl Default for HashRing {
    fn default() -> Self {
        HashRing {
            storage: Arc::new(Mutex::new(HashRingStorage::default())),
            config: Arc::new(HashRingConfig::default()),
        }
    }
}

impl<H: BuildHasher> HashRing<H> {
    pub fn with_config(config: HashRingConfig<H>) -> Self {
        let storage = Arc::new(Mutex::new(HashRingStorage::default()));
        HashRing {
            storage,
            config: Arc::new(config),
        }
    }

    pub fn find_partition_for_key<K: Hash>(&self, key: &K) -> Result<u64, HashRingError> {
        let mut hasher = self.config.get_hasher();
        key.hash(&mut hasher);
        Ok(hasher.finish() % self.config.partition_count())
    }

    pub fn list_nodes<N: FromStr>(&self) -> Result<Vec<N>, HashRingError> {
        let storage = self
            .storage
            .lock()
            .map_err(|e| HashRingError::StorageLock(e.to_string()))?;

        Ok(storage
            .list_nodes()
            .iter()
            .filter_map(|n| match String::from_utf8(n.clone()) {
                Ok(s) => match s.parse::<N>() {
                    Ok(n) => Some(n),
                    Err(_) => None,
                },
                Err(_) => None,
            })
            .collect())
    }

    pub fn average_load(&self) -> Result<f64, HashRingError> {
        let storage = self
            .storage
            .lock()
            .map_err(|e| HashRingError::StorageLock(e.to_string()))?;

        Ok(storage.average_load(self.config.partition_count(), self.config.load_factor()))
    }

    #[must_use]
    pub fn get_node_for_partition<N: FromStr>(&self, partition_id: u64) -> Option<N> {
        let storage = self
            .storage
            .lock()
            .map_err(|e| HashRingError::StorageLock(e.to_string()))
            .ok()?;

        storage
            .get_node_for_partition(partition_id)
            .and_then(|node| {
                String::from_utf8(node)
                    .ok()
                    .and_then(|s| s.parse::<N>().ok())
            })
    }

    pub fn locate_key<K: Hash, N: FromStr>(&self, key: &K) -> Option<N> {
        self.find_partition_for_key(key)
            .map(|partition_id| self.get_node_for_partition(partition_id))
            .unwrap_or(None)
    }

    pub fn load_distribution<N: FromStr + Hash + Eq>(
        &self,
    ) -> Result<HashMap<N, f64>, HashRingError> {
        let storage = self
            .storage
            .lock()
            .map_err(|e| HashRingError::StorageLock(e.to_string()))?;

        storage
            .load_distribution()
            .iter()
            .try_fold(
                HashMap::new(),
                |mut acc, (node, load)| match String::from_utf8(node.clone()) {
                    Ok(s) => match s.parse::<N>() {
                        Ok(n) => {
                            acc.insert(n, *load);
                            Ok(acc)
                        }
                        Err(_) => Err(HashRingError::InvalidValue(format!(
                            "could not parse string ({}) into type({})",
                            s,
                            std::any::type_name::<N>(),
                        ))),
                    },
                    Err(e) => Err(HashRingError::InvalidValue(format!(
                        "could not parse vec<u8> into string; received error: {e}"
                    ))),
                },
            )
    }

    pub fn add_nodes<N: Display>(&self, nodes: Vec<N>) -> Result<(), HashRingError> {
        let mut storage = self
            .storage
            .lock()
            .map_err(|e| HashRingError::StorageLock(e.to_string()))?;

        let mut uid = String::new();

        let ring_members = nodes
            .iter()
            .flat_map(|n| {
                (0..self.config.replication_factor())
                    .map(|i| {
                        let mut hasher = self.config.get_hasher();
                        write!(&mut uid, "{n} (hashring_node_replica_{i})").unwrap();
                        uid.hash(&mut hasher);
                        (hasher.finish(), n.to_string().as_bytes().to_vec())
                    })
                    .collect::<Vec<(u64, Vec<u8>)>>()
            })
            .collect::<Vec<(u64, Vec<u8>)>>();

        storage.add_nodes(
            nodes
                .iter()
                .map(|n| n.to_string().as_bytes().to_vec())
                .collect(),
            ring_members,
        );

        let node_max_capacity =
            storage.average_load(self.config.partition_count(), self.config.load_factor());

        let partitions_by_hash = (0..self.config.partition_count())
            .map(|partition_id| {
                let mut hasher = self.config.get_hasher();
                partition_id.hash(&mut hasher);
                let partition_hash = hasher.finish();
                (partition_hash, partition_id)
            })
            .collect::<BTreeMap<u64, u64>>();

        storage.distribute_partitions(partitions_by_hash, node_max_capacity);
        Ok(())
    }

    pub fn remove_nodes<N: Display>(&self, nodes: Vec<N>) -> Result<(), HashRingError> {
        let mut storage = self
            .storage
            .lock()
            .map_err(|e| HashRingError::StorageLock(e.to_string()))?;

        let mut uid = String::new();

        let removed_ring_keys = nodes
            .iter()
            .flat_map(|n| {
                (0..self.config.replication_factor())
                    .map(|i| {
                        let mut hasher = self.config.get_hasher();
                        write!(&mut uid, "{n} (hashring_node_replica_{i})").unwrap();
                        uid.hash(&mut hasher);
                        hasher.finish()
                    })
                    .collect::<Vec<u64>>()
            })
            .collect::<Vec<u64>>();

        storage.remove_nodes(
            nodes
                .iter()
                .map(|n| n.to_string().as_bytes().to_vec())
                .collect(),
            removed_ring_keys,
        );

        if storage.nodes_count() == 0 {
            return Ok(());
        }

        let node_max_capacity =
            storage.average_load(self.config.partition_count(), self.config.load_factor());

        let partitions_by_hash = (0..self.config.partition_count())
            .map(|partition_id| {
                let mut hasher = self.config.get_hasher();
                partition_id.hash(&mut hasher);
                let partition_hash = hasher.finish();
                (partition_hash, partition_id)
            })
            .collect::<BTreeMap<u64, u64>>();

        storage.distribute_partitions(partitions_by_hash, node_max_capacity);
        Ok(())
    }

    pub fn has_node<N: Display>(&self, node: N) -> Result<bool, HashRingError> {
        let storage = self
            .storage
            .lock()
            .map_err(|e| HashRingError::StorageLock(e.to_string()))?;

        Ok(storage.has_node(node.to_string().as_bytes().to_vec()))
    }

    pub fn is_empty(&self) -> Result<bool, HashRingError> {
        let storage = self
            .storage
            .lock()
            .map_err(|e| HashRingError::StorageLock(e.to_string()))?;

        Ok(storage.nodes_count() > 0)
    }

    pub fn nodes_count(&self) -> Result<usize, HashRingError> {
        let storage = self
            .storage
            .lock()
            .map_err(|e| HashRingError::StorageLock(e.to_string()))?;

        Ok(storage.nodes_count())
    }

    pub fn get_closest_nodes_for_partition<N: FromStr>(
        &self,
        partition_id: u64,
        count: usize,
    ) -> Result<Vec<N>, HashRingError> {
        let storage = self
            .storage
            .lock()
            .map_err(|e| HashRingError::StorageLock(e.to_string()))?;

        let members_by_hash = storage.list_nodes().iter().fold(
            BTreeMap::new(),
            |mut acc: BTreeMap<u64, Vec<u8>>, node| {
                let mut hasher = self.config.get_hasher();
                node.hash(&mut hasher);
                let node_hash = hasher.finish();
                acc.insert(node_hash, node.clone());
                acc
            },
        );

        let nodes =
            storage.get_closest_nodes_for_partition(members_by_hash, partition_id, count)?;

        Ok(nodes
            .iter()
            .filter_map(|n| {
                String::from_utf8(n.clone())
                    .ok()
                    .and_then(|s| s.parse::<N>().ok())
            })
            .collect())
    }
}

#[cfg(test)]
mod test {
    use crate::*;
    use std::fmt::Formatter;

    #[derive(Hash, PartialEq, Eq, Debug, Clone)]
    struct HashRingNode {
        uid: String,
    }

    impl HashRingNode {
        fn new(uid: String) -> Self {
            Self { uid }
        }
    }

    impl FromStr for HashRingNode {
        type Err = ();

        fn from_str(s: &str) -> Result<Self, Self::Err> {
            Ok(Self::new(s.to_string()))
        }
    }

    impl Display for HashRingNode {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            write!(f, "{}", self.uid)
        }
    }

    fn test_nodes(n: usize) -> Vec<HashRingNode> {
        (0..n)
            .map(|i| HashRingNode::new(format!("node_{i}")))
            .collect()
    }

    #[test]
    fn add_nodes() {
        let ring_config = HashRingConfig::new(23, 20, 1.25, RandomState::new());
        let ring = HashRing::with_config(ring_config);
        let test_node_count: usize = 8;
        let nodes = test_nodes(test_node_count);
        ring.add_nodes(nodes.clone()).unwrap();
        let ring_nodes = ring.list_nodes::<HashRingNode>().unwrap();

        assert_eq!(
            ring_nodes.len(),
            test_node_count,
            "ring nodes count is invalid after adding nodes. Expected {}, got {}",
            test_node_count,
            ring_nodes.len()
        );

        let mut expected_nodes = Vec::<HashRingNode>::new();
        for node in &nodes {
            for ring_node in &ring_nodes {
                if node == ring_node {
                    expected_nodes.push(node.clone());
                }
            }
        }
        assert_eq!(
            expected_nodes, nodes,
            "ring nodes are invalid after adding nodes. Expected {expected_nodes:?}, got {nodes:?}"
        );
    }

    #[test]
    fn remove_nodes() {
        let ring_config = HashRingConfig::new(23, 20, 1.25, RandomState::new());
        let ring = HashRing::with_config(ring_config);
        let test_node_count: usize = 8;
        let nodes = test_nodes(test_node_count);
        ring.add_nodes(nodes.clone()).unwrap();
        let ring_nodes = ring.list_nodes::<HashRingNode>().unwrap();

        assert_eq!(
            ring_nodes.len(),
            test_node_count,
            "ring nodes count is invalid after adding nodes. Expected {}, got {}",
            test_node_count,
            ring_nodes.len()
        );

        ring.remove_nodes(vec![nodes[0].clone()]).unwrap();
        let ring_nodes = ring.list_nodes::<HashRingNode>().unwrap();
        assert_eq!(
            ring_nodes.len(),
            test_node_count - 1,
            "ring nodes count is invalid after removing nodes. Expected {}, got {}",
            test_node_count - 1,
            ring_nodes.len()
        );

        ring.remove_nodes(nodes.clone()).unwrap();
        let ring_nodes = ring.list_nodes::<HashRingNode>().unwrap();
        assert_eq!(
            ring_nodes.len(),
            0,
            "ring nodes count is invalid after removing nodes. Expected {}, got {}",
            0,
            ring_nodes.len()
        );
    }

    #[test]
    fn load() {
        let ring_config = HashRingConfig::new(23, 20, 1.25, RandomState::new());
        let ring = HashRing::with_config(ring_config);
        assert_eq!(
            ring.average_load().unwrap(),
            0.0,
            "ring load is not 0 even though no nodes have been added."
        );
        let test_node_count: usize = 8;
        let nodes = test_nodes(test_node_count);
        ring.add_nodes(nodes.clone()).unwrap();
        let ring_nodes_count = ring.nodes_count().unwrap();
        assert_eq!(
            ring_nodes_count, test_node_count,
            "ring nodes count is invalid after adding nodes. Expected {test_node_count}, got {ring_nodes_count}"
        );

        let max_load = ring.average_load().unwrap();
        let load_distribution = ring.load_distribution::<HashRingNode>().unwrap();
        let has_overloaded_nodes = load_distribution.iter().any(|(_, load)| load > &max_load);
        assert!(!has_overloaded_nodes, "ring has overloaded nodes.",);
    }

    #[test]
    fn locate_key() {
        let ring_config = HashRingConfig::new(23, 20, 1.25, RandomState::new());
        let ring = HashRing::with_config(ring_config);
        let test_key = "iam_baman".to_string();

        assert_eq!(
            ring.locate_key::<String, HashRingNode>(&test_key),
            None,
            "ring supposedly has node for key {test_key} even though no nodes have been added."
        );

        let test_node_count: usize = 8;
        let nodes = test_nodes(test_node_count);
        ring.add_nodes(nodes.clone()).unwrap();

        assert!(
            ring.locate_key::<String, HashRingNode>(&test_key.to_string())
                .is_some(),
            "ring unable to locate node for key {test_key} even though {test_node_count} nodes have been added."
        );
    }

    #[test]
    fn insufficient_nodes() {
        let ring_config = HashRingConfig::new(23, 20, 1.25, RandomState::new());
        let ring = HashRing::with_config(ring_config);
        let test_node_count: usize = 8;
        let nodes = test_nodes(test_node_count);
        ring.add_nodes(nodes.clone()).unwrap();

        let test_key = "iam_baman".to_string();
        let test_key_partition = ring.find_partition_for_key(&test_key).unwrap();
        let closest_nodes_count = 30;
        assert_eq!(
            ring.get_closest_nodes_for_partition::<HashRingNode>(
                test_key_partition,
                closest_nodes_count
            ),
            Err(HashRingError::IllegalArgument(format!(
                "cannot get {closest_nodes_count} nodes for partition; there are only {test_node_count} nodes in the ring"
            ))),
        )
    }

    #[test]
    fn closest_nodes_for_key() {
        let ring_config = HashRingConfig::new(23, 20, 1.25, RandomState::new());
        let ring = HashRing::with_config(ring_config);
        let test_node_count: usize = 8;
        let nodes = test_nodes(test_node_count);
        ring.add_nodes(nodes.clone()).unwrap();

        let test_key = "iam_baman".to_string();
        let test_key_partition = ring.find_partition_for_key(&test_key).unwrap();
        let closest_nodes_count = 2;

        let closest_nodes = ring.get_closest_nodes_for_partition::<HashRingNode>(
            test_key_partition,
            closest_nodes_count,
        );

        assert!(
            closest_nodes.is_ok(),
            "ring unable to get closest {closest_nodes_count} nodes for key {test_key} even though {test_node_count} nodes have been added."
        );

        let closest_nodes = closest_nodes.unwrap();

        assert_eq!(
            closest_nodes.len(),
            closest_nodes_count,
            "ring returned closest {closest_nodes_count} nodes for key {test_key} instead of requested closest {test_node_count} nodes."
        );

        let node_for_partition = ring
            .get_node_for_partition::<HashRingNode>(ring.find_partition_for_key(&test_key).unwrap())
            .unwrap();

        assert_eq!(
            closest_nodes.iter().position(
                |node| *node == node_for_partition && node_for_partition != closest_nodes[0]
            ),
            None,
            "ring returned node {node_for_partition} for key {test_key} as closest node even though it is not."
        );
    }
}
