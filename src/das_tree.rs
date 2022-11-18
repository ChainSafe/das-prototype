use discv5::enr::{Enr, NodeId};
use dyn_clone::{clone_trait_object, DynClone};

use enr::EnrKey;
pub trait TreeNode<K: EnrKey + Send + Sync + Unpin + 'static>: DynClone {
    fn depth(&self) -> usize;
    fn id(&self) -> NodeId;
    fn score(&self) -> f64;
    fn sub_tree_size(&self) -> u64;
    // Add a node to the tree, return updated tree root, and ok == true if the node didn't already exist
    fn add(&self, n: Enr<K>) -> Result<Option<Box<dyn TreeNode<K>>>, Box<dyn std::error::Error>>;
    // Search for closest leaf nodes (log distance) and append to out,
    // maximum to the capacity of the out slice
    fn search(&self, target: NodeId, out: Vec<Box<dyn TreeNode<K>>>) -> Vec<Box<dyn TreeNode<K>>>;
    // Weakest finds the content with the weakest score at given tree depth
    fn weakest(&self, depth: u64) -> &dyn TreeNode<K>;
}
clone_trait_object!(<K:EnrKey + Send + Sync + Unpin + 'static>TreeNode<K>);
pub struct LeafNode<K: EnrKey + Send + Sync + Unpin + 'static> {
    pub depth: usize,
    pub score: f64,
    pub _self: Enr<K>,
}

impl<K: EnrKey + Send + Sync + Unpin + 'static> Clone for LeafNode<K> {
    fn clone(&self) -> Self {
        Self {
            depth: self.depth.clone(),
            score: self.score.clone(),
            _self: self._self.clone(),
        }
    }
}

pub struct PairNode<K: EnrKey + Send + Sync + Unpin + 'static> {
    pub depth: usize,
    pub score: f64,
    pub subtree_size: u64,

    // Bits after depth index are zeroed
    pub id: NodeId,

    // left and right are never nil at the same time

    // May be nil (pair node as extension node)
    pub left: Option<Box<dyn TreeNode<K>>>,
    // May be nil (pair node as extension node)
    pub right: Option<Box<dyn TreeNode<K>>>,
}

impl<K: EnrKey + Send + Sync + Unpin + 'static> Clone for PairNode<K> {
    fn clone(&self) -> Self {
        Self {
            depth: self.depth.clone(),
            score: self.score.clone(),
            subtree_size: self.subtree_size.clone(),
            id: self.id.clone(),
            left: self.left.clone(),
            right: self.right.clone(),
        }
    }
}

impl<K> TreeNode<K> for PairNode<K>
where
    K: EnrKey,
{
    fn depth(&self) -> usize {
        self.depth
    }

    fn id(&self) -> NodeId {
        self.id
    }

    fn score(&self) -> f64 {
        self.score
    }

    fn sub_tree_size(&self) -> u64 {
        self.subtree_size
    }

    fn add(&self, n: Enr<K>) -> Result<Option<Box<dyn TreeNode<K>>>, Box<dyn std::error::Error>> {
        let mut pair = self.clone();
        let mut ok = false;
        if pair.id().raw() == n.node_id().raw() {
            return Ok(Some(Box::new(pair)));
        }
        if bit_check(n.node_id(), self.depth()) {
            if let Some(right_node) = &self.right {
                let right = right_node.add(n)?;
                pair.right = right;
            } else {
                let leaf = LeafNode {
                    depth: pair.depth() + 1,
                    score: 0.0,
                    _self: n,
                };
                pair.right = Some(Box::new(leaf));
                ok = true;
            }
        } else {
            if let Some(left_node) = &self.left {
                let left = left_node.add(n)?;
                pair.left = left;
            } else {
                let leaf = LeafNode {
                    depth: pair.depth() + 1,
                    score: 0.0,
                    _self: n,
                };
                pair.left = Some(Box::new(leaf));
                ok = true;
            }
        }
        if ok {
            pair.subtree_size += 1;
            pair.score = 0.0;
            if let Some(left) = &pair.left {
                pair.score += left.score();
            }
            if let Some(right) = &pair.right {
                pair.score += right.score();
            }
        }
        return Ok(Some(Box::new(pair)));
    }

    fn search(&self, target: NodeId, out: Vec<Box<dyn TreeNode<K>>>) -> Vec<Box<dyn TreeNode<K>>> {
        if out.len() == out.capacity() {
            return out;
        }

        // what happens if theyre both nil? should be unreachable, otherwise why have a pair node?
        match (&self.left, &self.right) {
            (None, None) => unreachable!(),
            (None, Some(right)) => return right.search(target, out),
            (Some(left), None) => return left.search(target, out),
            (Some(left), Some(right)) => {
                if bit_check(target, self.depth()) {
                    let mut out = right.search(target, out);
                    if out.len() < out.capacity() {
                        out = left.search(target, out);
                    }
                    return out;
                } else {
                    let mut out = left.search(target, out);
                    if out.len() < out.capacity() {
                        out = right.search(target, out);
                    }
                    return out;
                }
            }
        }
    }

    fn weakest(&self, depth: u64) -> &dyn TreeNode<K> {
        if depth > self.depth.try_into().unwrap() {
            match (&self.left, &self.right) {
                (None, None) => return self,
                (None, Some(right)) => {
                    return right.weakest(depth);
                }
                (Some(left), None) => {
                    return left.weakest(depth);
                }
                (Some(left), Some(right)) => {
                    if right.score() > left.score() {
                        return right.weakest(depth);
                    } else {
                        return left.weakest(depth);
                    }
                }
            }
        }
        self
    }
}
impl<K: EnrKey + Send + Sync + Unpin + 'static> LeafNode<K> {
    pub fn new(enr: Enr<K>) -> Self {
        Self {
            depth: 0,
            score: 0.0,
            _self: enr,
        }
    }
}
impl<K> TreeNode<K> for LeafNode<K>
where
    K: EnrKey,
{
    fn depth(&self) -> usize {
        self.depth
    }
    fn score(&self) -> f64 {
        self.score
    }
    fn id(&self) -> NodeId {
        self._self.node_id()
    }
    fn sub_tree_size(&self) -> u64 {
        1
    }
    /// add a node and returns a pair node
    fn add(&self, n: Enr<K>) -> Result<Option<Box<dyn TreeNode<K>>>, Box<dyn std::error::Error>> {
        if self.id().raw() == n.node_id().raw() {
            return Err("wtf".into());
        }
        let pair = PairNode {
            depth: self.depth,
            score: 0.0,
            subtree_size: 0,
            id: clip(self.id(), self.depth),
            left: None,
            right: None,
        };
        pair.add(self._self.clone())?;
        pair.add(n.clone())?;
        Ok(Some(Box::new(pair)))
    }

    fn search(&self, _target: NodeId, out: Vec<Box<dyn TreeNode<K>>>) -> Vec<Box<dyn TreeNode<K>>> {
        if out.len() == out.capacity() {
            return out;
        }
        let mut out = out.clone();
        out.push(Box::new(self.clone()));
        out
    }

    fn weakest(&self, _depth: u64) -> &dyn TreeNode<K> {
        self
    }
}

fn bit_check(id: NodeId, bit_index: usize) -> bool {
    id.raw()[bit_index >> 3] & (1 << bit_index) != 0
}

fn clip(id: NodeId, depth: usize) -> NodeId {
    let i = depth >> 3;
    let mut raw_id = id.raw();
    // This will definitely panic lol
    raw_id[i] &= (1 << (depth as u8 & 7)) - 1;

    let mut j = i;
    while j < raw_id.len() {
        raw_id[j] = 0;
        j += 1;
    }

    return NodeId::new(&raw_id);
}
