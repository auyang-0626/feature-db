pub mod redo_log;

use std::sync::atomic::{AtomicU64, Ordering};

static T_ID: AtomicU64 = AtomicU64::new(0);

/// 生成事务ID
pub fn generate_tid() -> u64{
    T_ID.fetch_add(1,Ordering::AcqRel)
}