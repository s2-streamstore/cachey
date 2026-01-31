use std::{ops::Deref, str::FromStr};

use compact_str::CompactString;
use itertools::Itertools;
use serde::{Deserialize, Deserializer, Serialize, de::Error};

pub type PageId = u16;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BucketName(CompactString);

impl std::fmt::Display for BucketName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl BucketName {
    pub const MAX_LEN: usize = 64;

    pub fn new(name: impl Into<CompactString>) -> Result<Self, &'static str> {
        let name = name.into();
        if name.is_empty() {
            return Err("Bucket name cannot be empty");
        }
        if name.len() > Self::MAX_LEN {
            return Err("Bucket name too long");
        }
        Ok(Self(name))
    }
}

impl Deref for BucketName {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl AsRef<str> for BucketName {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
pub struct ObjectKind(CompactString);

impl std::fmt::Display for ObjectKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl ObjectKind {
    pub const MAX_LEN: usize = BucketName::MAX_LEN;

    pub fn new(key: impl Into<CompactString>) -> Result<Self, &'static str> {
        let key = key.into();
        if key.is_empty() {
            return Err("Object kind cannot be empty");
        }
        if key.len() > Self::MAX_LEN {
            return Err("Object kind too long");
        }
        Ok(Self(key))
    }
}

impl<'de> Deserialize<'de> for ObjectKind {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = CompactString::deserialize(deserializer)?;
        Self::new(s).map_err(D::Error::custom)
    }
}

impl FromStr for ObjectKind {
    type Err = &'static str;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::new(CompactString::from(s))
    }
}

impl Deref for ObjectKind {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<ObjectKind> for BucketName {
    fn from(value: ObjectKind) -> Self {
        BucketName(value.0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
pub struct ObjectKey(CompactString);

impl std::fmt::Display for ObjectKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl ObjectKey {
    pub const MAX_LEN: usize = 1024;

    pub fn new(key: impl Into<CompactString>) -> Result<Self, &'static str> {
        let key = key.into();
        if key.is_empty() {
            return Err("Object key cannot be empty");
        }
        if key.len() > Self::MAX_LEN {
            return Err("Object key too long");
        }
        Ok(Self(key))
    }
}

impl<'de> Deserialize<'de> for ObjectKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = CompactString::deserialize(deserializer)?;
        Self::new(s).map_err(D::Error::custom)
    }
}

impl FromStr for ObjectKey {
    type Err = &'static str;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::new(CompactString::from(s))
    }
}

impl Deref for ObjectKey {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BucketNameSet(Vec<BucketName>);

impl BucketNameSet {
    pub fn new(buckets: impl Iterator<Item = BucketName>) -> Result<Self, &'static str> {
        let deduped: Vec<BucketName> = buckets.unique().collect();
        if deduped.is_empty() {
            return Err("At least one bucket is required");
        }
        Ok(Self(deduped))
    }
}

impl std::ops::Deref for BucketNameSet {
    type Target = [BucketName];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl IntoIterator for BucketNameSet {
    type Item = BucketName;
    type IntoIter = std::vec::IntoIter<BucketName>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn object_kind_deserialize_rejects_empty() {
        let result: Result<ObjectKind, _> = serde_json::from_str(r#""""#);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("empty"));
    }

    #[test]
    fn object_kind_deserialize_rejects_too_long() {
        let long_str = "a".repeat(ObjectKind::MAX_LEN + 1);
        let json = format!(r#""{long_str}""#);
        let result: Result<ObjectKind, _> = serde_json::from_str(&json);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("too long"));
    }

    #[test]
    fn object_kind_deserialize_accepts_valid() {
        let result: Result<ObjectKind, _> = serde_json::from_str(r#""valid-kind""#);
        assert!(result.is_ok());
        assert_eq!(&*result.unwrap(), "valid-kind");
    }

    #[test]
    fn object_key_deserialize_rejects_empty() {
        let result: Result<ObjectKey, _> = serde_json::from_str(r#""""#);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("empty"));
    }

    #[test]
    fn object_key_deserialize_rejects_too_long() {
        let long_str = "a".repeat(ObjectKey::MAX_LEN + 1);
        let json = format!(r#""{long_str}""#);
        let result: Result<ObjectKey, _> = serde_json::from_str(&json);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("too long"));
    }

    #[test]
    fn object_key_deserialize_accepts_valid() {
        let result: Result<ObjectKey, _> = serde_json::from_str(r#""valid-key""#);
        assert!(result.is_ok());
        assert_eq!(&*result.unwrap(), "valid-key");
    }
}
