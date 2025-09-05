use std::{ops::Deref, str::FromStr};

use compact_str::CompactString;
use itertools::Itertools;
use serde::{Deserialize, Serialize};

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

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
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

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
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
