use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Single-producer, single-consumer lock-free ring buffer.
///
/// One OS thread pushes, another pops. No locks, no CAS loops.
/// Cache-line padding prevents false sharing between head and tail.
///
/// Capacity is rounded up to next power of two for fast modular indexing.
pub struct SpscQueue<T> {
    buffer: Box<[UnsafeCell<Option<T>>]>,
    mask: usize,
    // Producer writes head, consumer reads head
    _pad0: [u8; 56], // pad to separate cache lines
    head: AtomicUsize,
    _pad1: [u8; 56],
    tail: AtomicUsize,
    _pad2: [u8; 56],
}

// Safety: SpscQueue is designed for single-producer single-consumer access
// across two threads. The atomic head/tail ensure correct synchronization.
unsafe impl<T: Send> Send for SpscQueue<T> {}
unsafe impl<T: Send> Sync for SpscQueue<T> {}

impl<T> SpscQueue<T> {
    /// Create a new queue with at least `min_capacity` slots.
    pub fn new(min_capacity: usize) -> Self {
        let capacity = min_capacity.next_power_of_two().max(2);
        let mask = capacity - 1;
        let mut buffer = Vec::with_capacity(capacity);
        for _ in 0..capacity {
            buffer.push(UnsafeCell::new(None));
        }

        Self {
            buffer: buffer.into_boxed_slice(),
            mask,
            _pad0: [0; 56],
            head: AtomicUsize::new(0),
            _pad1: [0; 56],
            tail: AtomicUsize::new(0),
            _pad2: [0; 56],
        }
    }

    /// Number of usable slots.
    pub fn capacity(&self) -> usize {
        self.mask + 1
    }

    /// Try to push a value. Returns `Err(value)` if full.
    pub fn push(&self, value: T) -> Result<(), T> {
        let head = self.head.load(Ordering::Relaxed);
        let tail = self.tail.load(Ordering::Acquire);
        let size = head.wrapping_sub(tail);

        if size >= self.capacity() {
            return Err(value);
        }

        let slot = head & self.mask;
        // Safety: only the producer writes to the head slot, and we've verified
        // the consumer isn't occupying this slot.
        unsafe {
            *self.buffer[slot].get() = Some(value);
        }
        self.head.store(head.wrapping_add(1), Ordering::Release);
        Ok(())
    }

    /// Try to pop a value. Returns `None` if empty.
    pub fn pop(&self) -> Option<T> {
        let tail = self.tail.load(Ordering::Relaxed);
        let head = self.head.load(Ordering::Acquire);

        if tail == head {
            return None;
        }

        let slot = tail & self.mask;
        // Safety: only the consumer reads from the tail slot, and we've verified
        // the producer has written to this slot.
        let value = unsafe { (*self.buffer[slot].get()).take() };
        self.tail.store(tail.wrapping_add(1), Ordering::Release);
        value
    }

    /// Number of items currently in the queue.
    pub fn len(&self) -> usize {
        let head = self.head.load(Ordering::Acquire);
        let tail = self.tail.load(Ordering::Acquire);
        head.wrapping_sub(tail)
    }

    /// Whether the queue is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Producer handle. Can only push.
pub struct Producer<T> {
    queue: std::sync::Arc<SpscQueue<T>>,
}

/// Consumer handle. Can only pop.
pub struct Consumer<T> {
    queue: std::sync::Arc<SpscQueue<T>>,
}

/// Create a connected (producer, consumer) pair.
pub fn channel<T>(capacity: usize) -> (Producer<T>, Consumer<T>) {
    let queue = std::sync::Arc::new(SpscQueue::new(capacity));
    (
        Producer {
            queue: queue.clone(),
        },
        Consumer { queue },
    )
}

impl<T> Producer<T> {
    pub fn push(&self, value: T) -> Result<(), T> {
        self.queue.push(value)
    }

    pub fn is_full(&self) -> bool {
        self.queue.len() >= self.queue.capacity()
    }
}

impl<T> Consumer<T> {
    pub fn pop(&self) -> Option<T> {
        self.queue.pop()
    }

    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    /// Drain all available messages into a vec.
    pub fn drain(&self) -> Vec<T> {
        let mut items = Vec::new();
        while let Some(item) = self.pop() {
            items.push(item);
        }
        items
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn push_pop_basic() {
        let q = SpscQueue::new(4);
        assert!(q.is_empty());
        q.push(1).unwrap();
        q.push(2).unwrap();
        assert_eq!(q.len(), 2);
        assert_eq!(q.pop(), Some(1));
        assert_eq!(q.pop(), Some(2));
        assert!(q.is_empty());
    }

    #[test]
    fn full_queue_rejects() {
        let q = SpscQueue::new(2);
        q.push(1).unwrap();
        q.push(2).unwrap();
        assert_eq!(q.push(3), Err(3));
    }

    #[test]
    fn wraps_around() {
        let q = SpscQueue::new(2);
        for i in 0..100 {
            q.push(i).unwrap();
            assert_eq!(q.pop(), Some(i));
        }
    }

    #[test]
    fn capacity_rounds_up() {
        let q = SpscQueue::<u8>::new(3);
        assert_eq!(q.capacity(), 4);

        let q = SpscQueue::<u8>::new(5);
        assert_eq!(q.capacity(), 8);
    }

    #[test]
    fn channel_api() {
        let (tx, rx) = channel::<i32>(8);
        tx.push(10).unwrap();
        tx.push(20).unwrap();
        assert_eq!(rx.drain(), vec![10, 20]);
        assert!(rx.is_empty());
    }

    #[test]
    fn cross_thread() {
        let (tx, rx) = channel::<u64>(1024);
        let n = 10_000u64;

        let producer = std::thread::spawn(move || {
            for i in 0..n {
                while tx.push(i).is_err() {
                    std::hint::spin_loop();
                }
            }
        });

        let consumer = std::thread::spawn(move || {
            let mut received = Vec::with_capacity(n as usize);
            while received.len() < n as usize {
                if let Some(v) = rx.pop() {
                    received.push(v);
                } else {
                    std::hint::spin_loop();
                }
            }
            received
        });

        producer.join().unwrap();
        let received = consumer.join().unwrap();

        // Verify in-order delivery
        let expected: Vec<u64> = (0..n).collect();
        assert_eq!(received, expected);
    }
}
