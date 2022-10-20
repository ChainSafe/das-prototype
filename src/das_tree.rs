use discv5::enr::{Enr, NodeId};
use enr::EnrKey;
trait TreeNode<K: EnrKey> {
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
    fn weakest(&self, depth: u64) -> Box<dyn TreeNode<K>>;
}

struct LeafNode<K: EnrKey> {
    pub depth: usize,
    pub score: f64,
    pub _self: Enr<K>,
}

struct PairNode<K: EnrKey> {
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

impl<K> TreeNode<K> for PairNode<K>
where
    K: EnrKey,
{
    fn depth(&self) -> usize {
        todo!()
    }

    fn id(&self) -> NodeId {
        todo!()
    }

    fn score(&self) -> f64 {
        todo!()
    }

    fn sub_tree_size(&self) -> u64 {
        todo!()
    }

    fn add(&self, n: Enr<K>) -> Result<Option<Box<dyn TreeNode<K>>>, Box<dyn std::error::Error>> {
        todo!()
    }

    fn search(&self, target: NodeId, out: Vec<Box<dyn TreeNode<K>>>) -> Vec<Box<dyn TreeNode<K>>> {
        todo!()
    }

    fn weakest(&self, depth: u64) -> Box<dyn TreeNode<K>> {
        todo!()
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
        pair.add(self._self.clone());
        pair.add(n.clone());
        Ok(Some(Box::new(pair)))
    }

    fn search(&self, target: NodeId, out: Vec<Box<dyn TreeNode<K>>>) -> Vec<Box<dyn TreeNode<K>>> {
        todo!()
    }

    fn weakest(&self, depth: u64) -> Box<dyn TreeNode<K>> {
        todo!()
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
