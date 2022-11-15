use async_trait::async_trait;
use discv5_overlay::portalnet::types::content_key::{HistoryContentKey, OverlayContentKey};
use discv5_overlay::types::validation::Validator;
use enr::k256::sha2::Sha256;
use sha3::{Digest, Keccak256};
use ssz::{Decode, Encode};
use ssz_derive::{Decode, Encode};
use std::fmt;
use std::fmt::{Display, Formatter};

/// A content key in the DAS overlay network.
#[derive(Clone, Debug, Decode, Encode, PartialEq)]
#[ssz(enum_behaviour = "union")]
pub enum DASContentKey {
    Sample([u8; 32]),
}

#[allow(clippy::from_over_into)]
impl Into<Vec<u8>> for DASContentKey {
    fn into(self) -> Vec<u8> {
        self.as_ssz_bytes()
    }
}

impl TryFrom<Vec<u8>> for DASContentKey {
    type Error = &'static str;

    fn try_from(value: Vec<u8>) -> Result<Self, Self::Error> {
        match DASContentKey::from_ssz_bytes(&value) {
            Ok(key) => Ok(key),
            Err(_err) => {
                println!("unable to decode DASContentKey");
                Err("Unable to decode SSZ")
            }
        }
    }
}

impl Display for DASContentKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            Self::Sample(b) => format!("sample: {}", hex::encode(b)),
        };

        write!(f, "{}", s)
    }
}

impl OverlayContentKey for DASContentKey {
    fn content_id(&self) -> [u8; 32] {
        match self {
            DASContentKey::Sample(b) => b.clone(),
        }
    }
}

pub struct DASValidator;

#[async_trait]
impl Validator<DASContentKey> for DASValidator {
    async fn validate_content(
        &self,
        content_key: &DASContentKey,
        content: &[u8],
    ) -> anyhow::Result<()>
// where
        //     DASContentKey: 'async_trait,
    {
        match content_key {
            DASContentKey::Sample(_) => Ok(()),
        }
    }
}
