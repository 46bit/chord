use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;

pub trait NodeId: Copy + Debug {
    type Key: Eq + Ord + Hash + Copy + Debug;

    fn key(&self) -> Self::Key;
}

pub type NodeResult<T, I> = Result<T, I>;

#[derive(Clone, Debug)]
pub struct Node<I, T>
    where I: NodeId,
          T: Clone + Debug
{
    pub meta: NodeMeta<I>,
    pub items: HashMap<I::Key, T>,
}

#[derive(Copy, Clone, Debug, Serialize, Deserialize)]
pub struct NodeMeta<I>
    where I: NodeId
{
    pub id: I,
    pub successor_id: Option<I>,
    pub predecessor_id: Option<I>,
}

impl<I> NodeMeta<I>
    where I: NodeId
{
    pub fn owns(&self, key: I::Key) -> bool {
        // The lowest-keyed node is responsible for keys greater than the highest-keyed
        // node and for those less than itself.
        // Otherwise a node is merely responsible for keys greater than its predecessor.
        let predecessor_key = self.predecessor_id.expect("No predecessor set.").key();
        if predecessor_key > self.id.key() {
            key > predecessor_key || key <= self.id.key()
        } else {
            key > predecessor_key && key <= self.id.key()
        }
    }

    pub fn next(&self, _: I::Key) -> I {
        self.successor_id.expect("No successor set.")
    }
}

impl<I, T> Node<I, T>
    where I: NodeId,
          T: Clone + Debug
{
    pub fn new(id: I) -> Node<I, T> {
        Node {
            meta: NodeMeta {
                id: id,
                successor_id: None,
                predecessor_id: None,
            },
            items: HashMap::new(),
        }
    }

    pub fn exists(&self, key: I::Key) -> NodeResult<bool, I> {
        if self.meta.owns(key) {
            Ok(self.items.contains_key(&key))
        } else {
            Err(self.meta.next(key))
        }
    }

    pub fn get(&self, key: I::Key) -> NodeResult<Option<&T>, I> {
        if self.meta.owns(key) {
            Ok(self.items.get(&key))
        } else {
            Err(self.meta.next(key))
        }
    }

    pub fn set(&mut self, key: I::Key, value: T) -> NodeResult<(), I> {
        if self.meta.owns(key) {
            self.items.insert(key, value);
            Ok(())
        } else {
            Err(self.meta.next(key))
        }
    }

    pub fn delete(&mut self, key: I::Key) -> NodeResult<bool, I> {
        if self.meta.owns(key) {
            Ok(self.items.remove(&key).is_some())
        } else {
            Err(self.meta.next(key))
        }
    }
}

// impl<T> Rand for Node<T>
//     where T: Clone + Debug
// {
//     fn rand<R: Rng>(rng: &mut R) -> Node<T> {
//         Node::new(rng.gen())
//     }
// }

// impl<T> From<SocketAddr> for Node<T>
//     where T: Clone + Debug
// {
//     fn from(addr: SocketAddr) -> Node<T> {
//         let mut m = sha1::Sha1::new();
//         m.update(addr.to_string().as_bytes());
//         let u8_20 = m.digest().bytes();
//         let mut u32s = [0; 5];
//         for i in 0..5 {
//             let mut u32_: u32 = u8_20[i * 4] as u32;
//             u32_ = (u32_ << 8) | (u8_20[i * 4 + 1] as u32);
//             u32_ = (u32_ << 8) | (u8_20[i * 4 + 2] as u32);
//             u32_ = (u32_ << 8) | (u8_20[i * 4 + 3] as u32);
//             u32s[i] = u32_;
//         }
//         Node::new(u32s)
//     }
// }
