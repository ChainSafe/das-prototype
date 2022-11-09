use eyre::eyre;
use std::iter;
use tokio::sync::oneshot;

#[macro_export]
macro_rules! clone_all {
    ($i:ident) => {
        let $i = $i.clone();
    };
    ($i:ident, $($tt:tt)*) => {
        clone_all!($i);
        clone_all!($($tt)*);
    };
    ($this:ident . $i:ident) => {
        let $i = $this.$i.clone();
    };
    ($this:ident . $i:ident, $($tt:tt)*) => {
        clone_all!($this . $i);
        clone_all!($($tt)*);
    };
}

#[derive(Debug)]
pub enum MsgCountCmd {
    Reset,
    Get(oneshot::Sender<u64>),
    Increment,
}

pub fn encode_result_for_discv5(r: eyre::Result<Vec<u8>>) -> Vec<u8> {
    match r {
        Ok(mut b) => iter::once(1).chain(b).collect(),
        Err(e) => iter::once(0).chain(e.to_string().into_bytes()).collect(),
    }
}

pub fn decode_result_from_discv5(b: Vec<u8>) -> eyre::Result<Vec<u8>> {
    match b.first().unwrap() {
        1 => Ok(b[1..b.len()].to_vec()),
        0 => Err(eyre!(
            "error making discv5 request: {}",
            std::str::from_utf8(&b[1..b.len()]).unwrap()
        )),
        _ => panic!("unexpected message encoding"),
    }
}
