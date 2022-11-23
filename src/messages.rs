use crate::DASContentKey;
use discv5::Enr;
use ssz::{Decode, Encode};
use ssz_derive::{Decode, Encode};
use std::fmt;
use std::fmt::{Display, Formatter};
use discv5_overlay::portalnet::types::messages::SszEnr;

#[derive(Clone, Debug, Decode, Encode, PartialEq)]
#[ssz(enum_behaviour = "union")]
pub enum DisseminationMsg {
    Keys((u16, Vec<[u8; 32]>)),
    CloserNodes(Vec<SszEnr>),
    Samples(Vec<([u8; 32], Vec<u8>)>),
    Received(u16),
}
