//! VECTOR type and HNSW (Hierarchical Navigable Small World) index for
//! approximate nearest-neighbour search.
//!
//! The vector is stored as `Value::Text` with a canonical string representation
//! `[f32, f32, ...]` so that it fits the existing storage layer without
//! requiring new low-level Value variants. The HNSW index lives in memory
//! alongside the table and is used to accelerate ORDER BY vector_distance(...)
//! LIMIT queries.

use std::collections::{BinaryHeap, HashMap, HashSet};
use std::cmp::Reverse;

// -----------------------------------------------------------------------
// Vector helpers
// -----------------------------------------------------------------------

/// Parse a vector literal string `[1.0, 2.0, 3.0]` into a Vec<f32>.
pub fn parse_vector(s: &str) -> Option<Vec<f32>> {
    let s = s.trim();
    if !s.starts_with('[') || !s.ends_with(']') {
        return None;
    }
    let inner = &s[1..s.len() - 1];
    if inner.trim().is_empty() {
        return Some(Vec::new());
    }
    inner
        .split(',')
        .map(|part| part.trim().parse::<f32>().ok())
        .collect()
}

/// Format a vector as canonical string `[1, 2, 3]`.
pub fn format_vector(v: &[f32]) -> String {
    let parts: Vec<String> = v.iter().map(|x| format!("{x}")).collect();
    format!("[{}]", parts.join(", "))
}

/// Euclidean (L2) distance.
pub fn euclidean_distance(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len());
    a.iter()
        .zip(b.iter())
        .map(|(x, y)| (x - y) * (x - y))
        .sum::<f32>()
        .sqrt()
}

/// Cosine distance = 1 - cosine_similarity.
pub fn cosine_distance(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len());
    let dot: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
    let norm_a: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
    let norm_b: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();
    if norm_a == 0.0 || norm_b == 0.0 {
        return 1.0;
    }
    1.0 - dot / (norm_a * norm_b)
}

/// Supported distance metrics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DistanceMetric {
    Euclidean,
    Cosine,
}

/// Compute distance using the given metric.
pub fn vector_distance(a: &[f32], b: &[f32], metric: DistanceMetric) -> f32 {
    match metric {
        DistanceMetric::Euclidean => euclidean_distance(a, b),
        DistanceMetric::Cosine => cosine_distance(a, b),
    }
}

// -----------------------------------------------------------------------
// HNSW index
// -----------------------------------------------------------------------

/// HNSW parameters.
const HNSW_M: usize = 16; // max connections per node per layer
const HNSW_EF_CONSTRUCTION: usize = 200; // size of dynamic candidate list during build
// ln(16) ~= 2.7726 — precomputed since const fn can't call ln()
const HNSW_ML: f64 = 1.0 / 2.772588722239781;

/// A single node in the HNSW graph.
#[derive(Debug, Clone)]
#[allow(dead_code)]
struct HnswNode {
    id: usize,
    vector: Vec<f32>,
    /// Connections at each layer. connections[layer] = list of neighbor ids.
    connections: Vec<Vec<usize>>,
}

/// In-memory HNSW index.
#[derive(Debug)]
pub struct HnswIndex {
    nodes: Vec<HnswNode>,
    entry_point: Option<usize>,
    max_layer: usize,
    metric: DistanceMetric,
    #[allow(dead_code)]
    dimensions: usize,
    /// Column name this index is built on.
    pub column: String,
    /// Index name.
    pub name: String,
}

/// Ordered float for use in BinaryHeap.
#[derive(Debug, Clone, Copy)]
struct OrderedFloat(f32);

impl PartialEq for OrderedFloat {
    fn eq(&self, other: &Self) -> bool {
        self.0.to_bits() == other.0.to_bits()
    }
}
impl Eq for OrderedFloat {}
impl PartialOrd for OrderedFloat {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for OrderedFloat {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.partial_cmp(&other.0).unwrap_or(std::cmp::Ordering::Equal)
    }
}

impl HnswIndex {
    /// Create a new empty HNSW index.
    pub fn new(name: &str, column: &str, dimensions: usize, metric: DistanceMetric) -> Self {
        Self {
            nodes: Vec::new(),
            entry_point: None,
            max_layer: 0,
            metric,
            dimensions,
            column: column.to_string(),
            name: name.to_string(),
        }
    }

    /// Number of indexed vectors.
    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    /// Generate a random layer level for a new node.
    fn random_level(&self) -> usize {
        // Simple hash-based pseudo-random; avoids pulling in rand crate.
        let seed = self.nodes.len() as u64;
        let hash = seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        let r = (hash >> 33) as f64 / (1u64 << 31) as f64; // [0, 1)
        let level = (-r.ln() * HNSW_ML).floor() as usize;
        level.min(20) // cap to prevent degenerate cases
    }

    /// Insert a vector into the index.
    pub fn insert(&mut self, vector: Vec<f32>) -> usize {
        let id = self.nodes.len();
        let level = self.random_level();

        // Initialize node with empty connections at each layer
        let connections = (0..=level).map(|_| Vec::new()).collect();
        self.nodes.push(HnswNode {
            id,
            vector: vector.clone(),
            connections,
        });

        if self.entry_point.is_none() {
            self.entry_point = Some(id);
            self.max_layer = level;
            return id;
        }

        let ep = self.entry_point.unwrap();
        let mut current_ep = ep;

        // Phase 1: Greedily descend from the top layer down to level+1
        for lc in (level + 1..=self.max_layer).rev() {
            current_ep = self.greedy_search(current_ep, &vector, lc);
        }

        // Phase 2: Insert at layers [min(level, max_layer)..=0]
        let insert_top = level.min(self.max_layer);
        for lc in (0..=insert_top).rev() {
            let neighbors = self.search_layer(current_ep, &vector, HNSW_EF_CONSTRUCTION, lc);
            // Select M closest neighbors
            let selected: Vec<usize> = neighbors
                .iter()
                .take(HNSW_M)
                .map(|&(nid, _)| nid)
                .collect();

            // Add bidirectional connections
            self.nodes[id].connections[lc] = selected.clone();
            for &nid in &selected {
                if lc < self.nodes[nid].connections.len() {
                    self.nodes[nid].connections[lc].push(id);
                    // Prune if exceeds M
                    if self.nodes[nid].connections[lc].len() > HNSW_M {
                        let node_vec = self.nodes[nid].vector.clone();
                        let mut scored: Vec<(usize, f32)> = self.nodes[nid].connections[lc]
                            .iter()
                            .map(|&cid| {
                                let d = vector_distance(&node_vec, &self.nodes[cid].vector, self.metric);
                                (cid, d)
                            })
                            .collect();
                        scored.sort_by(|a, b| {
                            a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal)
                        });
                        self.nodes[nid].connections[lc] =
                            scored.into_iter().take(HNSW_M).map(|(cid, _)| cid).collect();
                    }
                }
            }

            if !neighbors.is_empty() {
                current_ep = neighbors[0].0;
            }
        }

        // Update entry point if new node has a higher level
        if level > self.max_layer {
            self.entry_point = Some(id);
            self.max_layer = level;
        }

        id
    }

    /// Greedy search at a single layer: return the single nearest neighbor.
    fn greedy_search(&self, start: usize, query: &[f32], layer: usize) -> usize {
        let mut current = start;
        let mut current_dist = vector_distance(&self.nodes[current].vector, query, self.metric);

        loop {
            let mut changed = false;
            if layer < self.nodes[current].connections.len() {
                for &nid in &self.nodes[current].connections[layer] {
                    let d = vector_distance(&self.nodes[nid].vector, query, self.metric);
                    if d < current_dist {
                        current = nid;
                        current_dist = d;
                        changed = true;
                    }
                }
            }
            if !changed {
                break;
            }
        }
        current
    }

    /// Search a single layer using a priority-queue based approach.
    /// Returns up to `ef` nearest neighbors as (id, distance), sorted by distance.
    fn search_layer(
        &self,
        start: usize,
        query: &[f32],
        ef: usize,
        layer: usize,
    ) -> Vec<(usize, f32)> {
        let start_dist = vector_distance(&self.nodes[start].vector, query, self.metric);

        // candidates: min-heap (closest first)
        let mut candidates: BinaryHeap<Reverse<(OrderedFloat, usize)>> = BinaryHeap::new();
        // results: max-heap (farthest first for eviction)
        let mut results: BinaryHeap<(OrderedFloat, usize)> = BinaryHeap::new();
        let mut visited: HashSet<usize> = HashSet::new();

        candidates.push(Reverse((OrderedFloat(start_dist), start)));
        results.push((OrderedFloat(start_dist), start));
        visited.insert(start);

        while let Some(Reverse((OrderedFloat(c_dist), c_id))) = candidates.pop() {
            // If closest candidate is farther than farthest result, stop
            if let Some(&(OrderedFloat(f_dist), _)) = results.peek()
                && c_dist > f_dist && results.len() >= ef
            {
                break;
            }

            if layer < self.nodes[c_id].connections.len() {
                for &nid in &self.nodes[c_id].connections[layer] {
                    if visited.contains(&nid) {
                        continue;
                    }
                    visited.insert(nid);

                    let d = vector_distance(&self.nodes[nid].vector, query, self.metric);

                    let should_add = if results.len() < ef {
                        true
                    } else if let Some(&(OrderedFloat(f_dist), _)) = results.peek() {
                        d < f_dist
                    } else {
                        true
                    };

                    if should_add {
                        candidates.push(Reverse((OrderedFloat(d), nid)));
                        results.push((OrderedFloat(d), nid));
                        if results.len() > ef {
                            results.pop(); // evict farthest
                        }
                    }
                }
            }
        }

        let mut result_vec: Vec<(usize, f32)> = results
            .into_iter()
            .map(|(OrderedFloat(d), id)| (id, d))
            .collect();
        result_vec.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
        result_vec
    }

    /// Query the index for the `k` nearest neighbors to `query`.
    /// Returns (node_id, distance) pairs sorted by distance.
    pub fn search(&self, query: &[f32], k: usize) -> Vec<(usize, f32)> {
        if self.nodes.is_empty() || self.entry_point.is_none() {
            return Vec::new();
        }

        let ep = self.entry_point.unwrap();
        let mut current_ep = ep;

        // Descend from top layer
        for lc in (1..=self.max_layer).rev() {
            current_ep = self.greedy_search(current_ep, query, lc);
        }

        // Search at layer 0 with ef = max(k, ef_construction)
        let ef = k.max(HNSW_EF_CONSTRUCTION);
        let mut results = self.search_layer(current_ep, query, ef, 0);
        results.truncate(k);
        results
    }
}

// -----------------------------------------------------------------------
// HNSW Index Registry — maps (table, column) -> HnswIndex
// -----------------------------------------------------------------------

/// Registry of HNSW indices, keyed by index name.
#[derive(Debug, Default)]
pub struct HnswRegistry {
    indices: HashMap<String, HnswIndex>,
    /// Maps (table_name, column_name) to index_name.
    lookup: HashMap<(String, String), String>,
}

impl HnswRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Create and register a new HNSW index.
    pub fn create_index(
        &mut self,
        name: &str,
        table: &str,
        column: &str,
        dimensions: usize,
        metric: DistanceMetric,
    ) -> &mut HnswIndex {
        let index = HnswIndex::new(name, column, dimensions, metric);
        self.indices.insert(name.to_string(), index);
        self.lookup
            .insert((table.to_string(), column.to_string()), name.to_string());
        self.indices.get_mut(name).unwrap()
    }

    /// Get an index by name.
    pub fn get(&self, name: &str) -> Option<&HnswIndex> {
        self.indices.get(name)
    }

    /// Get a mutable index by name.
    pub fn get_mut(&mut self, name: &str) -> Option<&mut HnswIndex> {
        self.indices.get_mut(name)
    }

    /// Find the HNSW index for a given table and column.
    pub fn find_for_column(&self, table: &str, column: &str) -> Option<&HnswIndex> {
        self.lookup
            .get(&(table.to_string(), column.to_string()))
            .and_then(|name| self.indices.get(name))
    }

    /// Check if an HNSW index exists for a given table and column.
    pub fn has_index_for(&self, table: &str, column: &str) -> bool {
        self.lookup
            .contains_key(&(table.to_string(), column.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_and_format_vector() {
        let v = parse_vector("[1.0, 2.0, 3.0]").unwrap();
        assert_eq!(v, vec![1.0, 2.0, 3.0]);
        let s = format_vector(&v);
        assert_eq!(s, "[1, 2, 3]");

        // Empty vector
        let v = parse_vector("[]").unwrap();
        assert!(v.is_empty());

        // Invalid
        assert!(parse_vector("not a vector").is_none());
        assert!(parse_vector("[1.0, abc]").is_none());
    }

    #[test]
    fn euclidean_distance_works() {
        let a = vec![0.0, 0.0, 0.0];
        let b = vec![3.0, 4.0, 0.0];
        assert!((euclidean_distance(&a, &b) - 5.0).abs() < 1e-6);
    }

    #[test]
    fn cosine_distance_works() {
        let a = vec![1.0, 0.0];
        let b = vec![0.0, 1.0];
        // Orthogonal vectors: cosine similarity = 0, distance = 1
        assert!((cosine_distance(&a, &b) - 1.0).abs() < 1e-6);

        // Same direction: distance = 0
        let c = vec![1.0, 0.0];
        assert!((cosine_distance(&a, &c) - 0.0).abs() < 1e-6);
    }

    #[test]
    fn cosine_distance_zero_vector() {
        let a = vec![0.0, 0.0];
        let b = vec![1.0, 0.0];
        assert_eq!(cosine_distance(&a, &b), 1.0);
    }

    #[test]
    fn hnsw_basic_insert_and_search() {
        let mut index = HnswIndex::new("test_idx", "embedding", 3, DistanceMetric::Euclidean);

        // Insert some vectors
        let vectors = vec![
            vec![1.0, 0.0, 0.0],
            vec![0.0, 1.0, 0.0],
            vec![0.0, 0.0, 1.0],
            vec![1.0, 1.0, 0.0],
            vec![0.0, 1.0, 1.0],
            vec![1.0, 0.0, 1.0],
            vec![1.0, 1.0, 1.0],
        ];
        for v in &vectors {
            index.insert(v.clone());
        }
        assert_eq!(index.len(), 7);

        // Search for nearest to [1, 0, 0] — should find itself
        let results = index.search(&[1.0, 0.0, 0.0], 3);
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].0, 0); // exact match
        assert!(results[0].1 < 1e-6); // distance ~0
    }

    #[test]
    fn hnsw_search_respects_k() {
        let mut index = HnswIndex::new("test_idx", "emb", 2, DistanceMetric::Euclidean);
        for i in 0..20 {
            index.insert(vec![i as f32, 0.0]);
        }
        let results = index.search(&[5.0, 0.0], 5);
        assert_eq!(results.len(), 5);
        // Closest should be 5.0 (distance 0)
        assert_eq!(results[0].0, 5);
        assert!(results[0].1 < 1e-6);
    }

    #[test]
    fn hnsw_empty_search() {
        let index = HnswIndex::new("test_idx", "emb", 2, DistanceMetric::Euclidean);
        let results = index.search(&[1.0, 2.0], 5);
        assert!(results.is_empty());
    }

    #[test]
    fn hnsw_cosine_metric() {
        let mut index = HnswIndex::new("test_idx", "emb", 2, DistanceMetric::Cosine);
        index.insert(vec![1.0, 0.0]);
        index.insert(vec![0.0, 1.0]);
        index.insert(vec![0.707, 0.707]); // ~45 degrees

        // Search for direction close to [1, 0] — should find [1, 0] first
        let results = index.search(&[0.9, 0.1], 3);
        assert_eq!(results[0].0, 0); // [1, 0] closest by cosine
    }

    #[test]
    fn hnsw_registry() {
        let mut reg = HnswRegistry::new();
        reg.create_index("idx1", "items", "embedding", 3, DistanceMetric::Euclidean);
        assert!(reg.has_index_for("items", "embedding"));
        assert!(!reg.has_index_for("items", "name"));
        assert!(reg.find_for_column("items", "embedding").is_some());
        assert!(reg.get("idx1").is_some());
    }
}
