use std::collections::hash_map::{DefaultHasher, RandomState};
use std::hash::BuildHasher;
use std::mem;
use wd_tools::MD5;

pub const FILTER_PREFIX: &'static str = "SFP";

pub fn assembly_prefix(appid: &str, group: &str) -> String {
    format!("{}_{}_{}", FILTER_PREFIX, appid, group)
}
pub fn analyze_prefix(key: &str) -> Option<(String, String)> {
    let mut list = key.split("_").collect::<Vec<_>>();
    if list.len() < 3 || list[0] != FILTER_PREFIX {
        None
    } else {
        Some((list.remove(1).to_string(), list.remove(1).to_string()))
    }
}
#[allow(dead_code)]
struct MyRandomState {
    k0: u64,
    k1: u64,
}
pub fn generate_hasher(seed: &str) -> DefaultHasher {
    let bytes = seed.md5();
    let k0 = u64::from_le_bytes([
        bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
    ]);
    let k1 = u64::from_le_bytes([
        bytes[8], bytes[9], bytes[10], bytes[11], bytes[12], bytes[13], bytes[14], bytes[15],
    ]);
    let mrs = MyRandomState { k0, k1 };

    let bytes: &[u8] = unsafe {
        let ptr = &mrs as *const MyRandomState as *const u8;
        std::slice::from_raw_parts(ptr, mem::size_of::<MyRandomState>())
    };

    let builder: RandomState = unsafe { std::ptr::read(bytes.as_ptr() as *const _) };
    builder.build_hasher()
}

#[cfg(test)]
mod test {
    use crate::util::generate_hasher;
    use std::hash::Hasher;

    #[test]
    fn test_generate_hasher() {
        let mut hasher = generate_hasher("123");

        hasher.write("123".as_bytes());
        let result = hasher.finish();
    }
}
