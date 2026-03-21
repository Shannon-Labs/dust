use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SchemaFingerprint(pub String);

impl SchemaFingerprint {
    pub fn compute(input: impl AsRef<[u8]>) -> Self {
        let digest = blake3::hash(input.as_ref()).to_hex().to_string();
        Self(format!("sch_{}", &digest[..12]))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Default for SchemaFingerprint {
    fn default() -> Self {
        Self::compute([])
    }
}
