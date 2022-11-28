use crate::DASContentKey;
use discv5::Enr;
use ssz::{Decode, Encode};
use ssz_derive::{Decode, Encode};
use std::fmt;
use std::fmt::{Debug, Display, Formatter, Pointer};
use discv5_overlay::portalnet::types::messages::SszEnr;
use enr::NodeId;
use itertools::Itertools;

#[derive(Clone, Decode, Encode, PartialEq)]
#[ssz(enum_behaviour = "union")]
pub enum DisseminationMsg {
    Keys((u16, Vec<[u8; 32]>)),
    CloserNodes(Vec<SszEnr>),
    Samples(Vec<([u8; 32], Vec<u8>)>),
    Received(u16),
}

impl Debug for DisseminationMsg {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            DisseminationMsg::Keys((request_id, keys)) => {
                write!(f, "Keys({request_id}, {:?})", keys.into_iter().map(|e| NodeId::new(e).to_string()).collect_vec())
            }
            DisseminationMsg::CloserNodes(enrs) => write!(f, "CloserNodes({:?})", enrs),
            DisseminationMsg::Samples(samples) => {
                write!(f, "Samples({:?})", samples.into_iter().map(|(key, val)| (NodeId::new(key).to_string(), hex::encode(val))).collect_vec())
            }
            DisseminationMsg::Received(_) => write!(f, "Received"),
        }
    }
}
