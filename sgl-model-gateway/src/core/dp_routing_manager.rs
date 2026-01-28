//! DP (Data Parallel) routing manager for maintaining main_key -> (worker_url, dp_rank) mappings
//! and tracking DP load per worker

use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use dashmap::DashMap;

/// Mapping from main_key to (worker_url, dp_rank)
type MainKeyMapping = DashMap<String, (String, usize)>;

/// Load tracking for each worker's DP ranks
/// worker_url -> dp_rank -> load_count
type WorkerDpLoad = DashMap<String, Arc<DashMap<usize, usize>>>;

/// DP routing manager that maintains main_key mappings and tracks DP load
#[derive(Debug, Clone)]
pub struct DpRoutingManager {
    /// DP size (number of DP ranks per worker)
    dp_size: usize,
    /// Mapping from main_key to (worker_url, dp_rank)
    key_to_worker_dp: Arc<MainKeyMapping>,
    /// Load tracking per worker per DP rank
    worker_dp_loads: Arc<WorkerDpLoad>,
}

impl DpRoutingManager {
    /// Create a new DP routing manager with the given DP size
    pub fn new(dp_size: usize) -> Self {
        Self {
            dp_size,
            key_to_worker_dp: Arc::new(DashMap::new()),
            worker_dp_loads: Arc::new(DashMap::new()),
        }
    }

    /// Get DP size
    pub fn dp_size(&self) -> usize {
        self.dp_size
    }

    /// Get the worker URL and DP rank for a main_key if it exists
    pub fn get_worker_dp(&self, main_key: &str) -> Option<(String, usize)> {
        self.key_to_worker_dp.get(main_key).map(|entry| entry.value().clone())
    }

    /// Assign a worker URL and DP rank to a main_key
    /// This also increments the load counter for the assigned (worker_url, dp_rank)
    pub fn assign_worker_dp(&self, main_key: &str, worker_url: String, dp_rank: usize) {
        // Validate dp_rank is within bounds
        if dp_rank >= self.dp_size {
            tracing::warn!(
                main_key = %main_key,
                worker_url = %worker_url,
                dp_rank = dp_rank,
                dp_size = self.dp_size,
                "DP rank out of bounds, clamping to max"
            );
            return;
        }

        // Remove old mapping if exists
        if let Some((old_worker_url, old_dp_rank)) = self.key_to_worker_dp.remove(main_key) {
            self.decrement_load(&old_worker_url, old_dp_rank);
        }

        // Set new mapping
        self.key_to_worker_dp
            .insert(main_key.to_string(), (worker_url.clone(), dp_rank));

        // Increment load
        self.increment_load(&worker_url, dp_rank);
    }

    /// Remove a main_key mapping
    /// Returns true if the key was removed, false if it didn't exist
    pub fn remove_key(&self, main_key: &str) -> bool {
        if let Some((worker_url, dp_rank)) = self.key_to_worker_dp.remove(main_key) {
            self.decrement_load(&worker_url, dp_rank);
            true
        } else {
            false
        }
    }

    /// Get the DP rank with the lowest load for a given worker
    /// Returns None if the worker has no load tracking (shouldn't happen in normal operation)
    pub fn get_least_loaded_dp_rank(&self, worker_url: &str) -> Option<usize> {
        let dp_loads = self.worker_dp_loads.get(worker_url)?;
        
        let mut min_load = usize::MAX;
        let mut best_rank = 0;

        for rank in 0..self.dp_size {
            let load = dp_loads.get(&rank).map(|e| *e.value()).unwrap_or(0);
            if load < min_load {
                min_load = load;
                best_rank = rank;
            }
        }

        Some(best_rank)
    }

    /// Initialize load tracking for a worker if it doesn't exist
    fn ensure_worker_load_tracking(&self, worker_url: &str) {
        self.worker_dp_loads
            .entry(worker_url.to_string())
            .or_insert_with(|| Arc::new(DashMap::new()));
    }

    /// Increment load for a (worker_url, dp_rank) pair
    fn increment_load(&self, worker_url: &str, dp_rank: usize) {
        self.ensure_worker_load_tracking(worker_url);
        
        if let Some(dp_loads) = self.worker_dp_loads.get(worker_url) {
            *dp_loads.entry(dp_rank).or_insert(0) += 1;
        }
    }

    /// Decrement load for a (worker_url, dp_rank) pair
    fn decrement_load(&self, worker_url: &str, dp_rank: usize) {
        if let Some(dp_loads) = self.worker_dp_loads.get(worker_url) {
            if let Some(mut entry) = dp_loads.get_mut(&dp_rank) {
                if *entry > 0 {
                    *entry -= 1;
                    if *entry == 0 {
                        drop(entry);
                        dp_loads.remove(&dp_rank);
                    }
                }
            }
        }
    }

    /// Get load statistics for a worker
    /// Returns a vector of (dp_rank, load_count) pairs
    pub fn get_worker_loads(&self, worker_url: &str) -> Vec<(usize, usize)> {
        if let Some(dp_loads) = self.worker_dp_loads.get(worker_url) {
            (0..self.dp_size)
                .map(|rank| (rank, dp_loads.get(&rank).map(|e| *e.value()).unwrap_or(0)))
                .collect()
        } else {
            (0..self.dp_size).map(|rank| (rank, 0)).collect()
        }
    }

    /// Get all main_key mappings (for debugging/admin purposes)
    pub fn get_all_mappings(&self) -> HashMap<String, (String, usize)> {
        self.key_to_worker_dp
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().clone()))
            .collect()
    }

    /// Clear all mappings (for testing/cleanup)
    pub fn clear(&self) {
        self.key_to_worker_dp.clear();
        self.worker_dp_loads.clear();
    }
}