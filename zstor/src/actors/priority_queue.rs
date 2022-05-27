use std::collections::VecDeque;
/// A min heap implementation
pub struct PriorityQueue<T> {
    // prefered this over BinaryHeap for:
    // - stability
    // - it's three levels now any way and it (probably) won't grow much bigger
    queues: Vec<VecDeque<T>>,
}

impl<T> PriorityQueue<T> {
    /// constructor receiving the number of priority levels, lower is more prior
    pub fn new(levels: u16) -> Self {
        let mut queues = Vec::new();
        for _ in 0..levels.into() {
            queues.push(VecDeque::new());
        }
        PriorityQueue { queues }
    }
    /// add a new entry with the given priority
    pub fn push(&mut self, priority: u16, obj: T) {
        self.queues[priority as usize].push_back(obj)
    }

    /// repush is for retrying. it pushes the entry at the beginning of its priority queue
    pub fn repush(&mut self, priority: u16, obj: T) {
        self.queues[priority as usize].push_front(obj)
    }

    /// pop the entry with least priority
    pub fn pop(&mut self) -> Option<T> {
        for q in self.queues.iter_mut() {
            if q.len() != 0 {
                return q.pop_front();
            }
        }
        return None;
    }
}
