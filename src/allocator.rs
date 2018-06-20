

use memory::{Storage, Address, Size};
use persist::{Serialize, StorageWriter};

#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct Allocation {
    pub addr: Address,
    pub size: Size,
}

impl Allocation {
    #[inline]
    pub fn new(addr: Address, size: Size) -> Allocation {
        Allocation {
            addr,
            size,
        }
    }

    #[inline]
    pub fn start(&self) -> Address {
        self.addr
    }

    #[inline]
    pub fn end(&self) -> Address {
        self.addr + self.size
    }
}

impl Serialize for Allocation {
    #[inline]
    fn write<'s, S: Storage + 's>(&self, writer: &mut StorageWriter<'s, S>) {
        let Allocation {
            addr,
            size,
        } = *self;

        addr.write(writer);
        size.write(writer);
    }
}

pub struct Allocator {
    allocations: Vec<Allocation>,
    free_by_addr: Vec<Allocation>,
    free_by_size: Vec<Allocation>,
    total_size: Size,

    // TODO: this could be optimized by using an interval tree
    live_mem_refs: Vec<LiveMemRef>,
}

impl Allocator {

    pub fn new(total_size: Size) -> Allocator {
        Allocator {
            allocations: vec![],
            free_by_addr: vec![Allocation::new(Address(0), total_size)],
            free_by_size: vec![Allocation::new(Address(0), total_size)],
            total_size,
            live_mem_refs: vec![],
        }
    }

    pub fn total_size(&self) -> Size {
        self.total_size
    }

    pub fn max_addr(&self) -> Address {
        let last_allocation = self.allocations.last().unwrap();
        last_allocation.end()
    }

    pub fn alloc(&mut self, size: Size) -> Allocation {
        assert!(size != Size(0));

        match self.find_free_by_size(size) {
            Ok(index) => {
                let alloc = self.free_by_size.remove(index);
                self.remove_free_by_addr(alloc);
                self.insert_alloc(alloc);
                alloc
            }
            Err(index) => {
                // Next best fit.
                if index == self.free_by_size.len() {
                    println!("{:?}, size={:?}", self.free_by_size, size);
                }

                let available_alloc = self.free_by_size[index];
                assert!(available_alloc.size >= size);

                self.free_by_size.remove(index);
                let remaining_space = available_alloc.size - size;
                let remaining_free_alloc = Allocation::new(available_alloc.start() + size, remaining_space);
                self.insert_free_by_size(remaining_free_alloc);
                match self.find_free_by_address(available_alloc.addr) {
                    Ok(index) => {
                        self.free_by_addr[index] = remaining_free_alloc;
                        self.assert_order_free_by_addr(index);
                    }
                    Err(_) => {
                        panic!("Mismatch between alloc_by_size and alloc_by_addr.")
                    }
                }

                let new_alloc = Allocation::new(available_alloc.start(), size);
                assert_eq!(new_alloc.end(), available_alloc.start() + size);
                self.insert_alloc(new_alloc);
                new_alloc
            }
        }
    }

    pub fn free(&mut self, freed_alloc: Allocation) {
        let addr = freed_alloc.addr;
        if let Ok(alloc_index) = self.find_alloc_by_address(addr) {
            let alloc = self.allocations.remove(alloc_index);
            assert_eq!(alloc, freed_alloc, "Allocations differ in size.");
        } else {
            panic!("Could not find allocation at {:?}", addr);
        };

        match self.find_free_by_address(addr) {
            Ok(index) => {
                panic!("Free-list already contains allocation ({:?}) at {:?}", self.free_by_addr[index], addr);
            }
            Err(index) => {
                if index == self.free_by_addr.len() {
                    self.free_by_addr.push(freed_alloc);
                    self.assert_order_free_by_addr(index);
                    self.insert_free_by_size(freed_alloc);
                }

                {
                    let next_free_alloc = self.free_by_addr[index];

                    if freed_alloc.end() == next_free_alloc.start() {
                        self.remove_free_by_size(next_free_alloc);
                        let replacement = Allocation::new(freed_alloc.start(),
                                                          next_free_alloc.size + freed_alloc.size);
                        self.free_by_addr[index] = replacement;
                        self.assert_order_free_by_addr(index);
                        self.insert_free_by_size(replacement);
                        return
                    }
                }

                if index > 0 {
                    let prev_free_alloc = self.free_by_addr[index - 1];

                    if prev_free_alloc.end() == freed_alloc.start() {
                        self.remove_free_by_size(prev_free_alloc);
                        let replacement = Allocation::new(prev_free_alloc.start(),
                                                          prev_free_alloc.size + freed_alloc.size);
                        self.free_by_addr[index - 1] = replacement;
                        self.assert_order_free_by_addr(index - 1);
                        self.insert_free_by_size(replacement);
                        return
                    }
                }

                self.free_by_addr.insert(index, freed_alloc);
                self.assert_order_free_by_addr(index);
                self.insert_free_by_size(freed_alloc);
            }
        }
    }

    fn find_free_by_size(&self, size: Size) -> Result<usize, usize> {
        self.free_by_size.binary_search_by_key(&size, |alloc| alloc.size)
    }

    fn insert_free_by_size(&mut self, alloc: Allocation) {

        match self.free_by_size.binary_search_by_key(&alloc.size, |alloc| alloc.size) {
            Ok(mut index) => {
                while self.free_by_size[index].addr < alloc.addr && self.free_by_size[index].size == alloc.size {
                    index += 1;
                }

                assert_ne!(alloc, self.free_by_size[index]);

                self.free_by_size.insert(index, alloc);
            }
            Err(index) => {
                self.free_by_size.insert(index, alloc);
            }
        };
    }

    fn remove_free_by_size(&mut self, alloc: Allocation) {
        match self.free_by_size.binary_search_by_key(&alloc.size, |alloc| alloc.size) {
            Ok(start_index) => {
                // We might have landed in the middle of block of allocations with
                // the same size, so we have to search forward and backward.

                // Search forward from start_index:
                let mut index = start_index;
                loop {
                    if self.free_by_size[index].addr == alloc.addr {
                        assert_eq!(self.free_by_size.remove(index), alloc);
                        return
                    }

                    index += 1;

                    if index == self.free_by_size.len() || self.free_by_size[index].size != alloc.size {
                        break;
                    }
                }

                // search backwards from start_index
                if start_index > 0 && self.free_by_size[start_index - 1].size == alloc.size {
                    index = start_index - 1;
                    loop {
                        if self.free_by_size[index].addr == alloc.addr {
                            assert_eq!(self.free_by_size.remove(index), alloc);
                            return
                        }

                        if index == 0 || self.free_by_size[index - 1].size != alloc.size {
                            break;
                        }

                        index -= 1;
                    }
                }

                unreachable!("We should have found the allocation with the correct address.")
            }
            Err(_) => {
                panic!("Allocation not found. No allocation with the given size.")
            }
        };
    }

    fn remove_free_by_addr(&mut self, alloc: Allocation) {
        match self.free_by_addr.binary_search_by_key(&alloc.addr, |alloc| alloc.addr) {
            Ok(index) => {
                assert_eq!(self.free_by_addr.remove(index), alloc);
            }
            Err(_) => {
                panic!("Allocation not found. No allocation with the given addr.")
            }
        };
    }

    fn find_free_by_address(&self, addr: Address) -> Result<usize, usize> {
        self.free_by_addr.binary_search_by_key(&addr, |alloc| alloc.addr)
    }

    fn find_alloc_by_address(&self, addr: Address) -> Result<usize, usize> {
        self.allocations.binary_search_by_key(&addr, |alloc| alloc.addr)
    }

    fn insert_alloc(&mut self, alloc: Allocation) {
        match self.find_alloc_by_address(alloc.addr) {
            Ok(_) => {
                panic!("Allocation at {:?} already exists.", alloc.addr);
            }
            Err(index) => {
                self.allocations.insert(index, alloc);
            }
        }
    }

    fn assert_order_free_by_addr(&self, index: usize) {
        if index > 0 {
            assert!(self.free_by_addr[index - 1].addr < self.free_by_addr[index].addr);
        }

        if index < self.free_by_addr.len() - 1 {
            assert!(self.free_by_addr[index + 1].addr > self.free_by_addr[index].addr)
        }
    }

    pub(crate) fn register_mem_ref(&mut self, addr: Address, len: Size, mutable: bool) -> LiveMemRef {
        let new_mem_ref = LiveMemRef::new(addr, len, mutable);

        // Find allocation
        let alloc_index = match self.find_alloc_by_address(addr) {
            Ok(index) => index,
            Err(index) => {
                assert!(index > 0);
                index - 1
            }
        };

        // Check that we have an allocation
        assert!(alloc_index < self.allocations.len());

        // Check that the borrowed range does not extend beyond the allocation
        assert!(new_mem_ref.end <= self.allocations[alloc_index].end());

        // Check that we don't conflict with any other borrowed range
        assert!(!self.live_mem_refs.iter().any(|lmr| lmr.conflicts_with(&new_mem_ref)));

        self.live_mem_refs.push(new_mem_ref);

        new_mem_ref
    }

    pub(crate) fn unregister_mem_ref(&mut self, mem_ref: LiveMemRef) {
        let idx = self.live_mem_refs.iter().rposition(|&x| x == mem_ref).expect("wat?!");

        let last_index = self.live_mem_refs.len() - 1;

        if idx != last_index {
            self.live_mem_refs[idx] = self.live_mem_refs[last_index];
        }

        self.live_mem_refs.pop();
    }
}

impl Serialize for Allocator {
    #[inline]
    fn write<'s, S: Storage + 's>(&self, writer: &mut StorageWriter<'s, S>) {
        let Allocator {
            ref allocations,
            ref free_by_addr,
            ref free_by_size,
            total_size,
            live_mem_refs: _,
        } = *self;

        allocations.write(writer);
        free_by_addr.write(writer);
        free_by_size.write(writer);
        total_size.write(writer);
    }
}

#[derive(Copy, Clone, PartialEq, Eq)]
pub(crate) struct LiveMemRef {
    start: Address,
    end: Address,
    mutable: bool,
}

impl LiveMemRef {

    fn new(start: Address, len: Size, mutable: bool) -> LiveMemRef {
        LiveMemRef {
            start,
            end: start + len,
            mutable,
        }
    }

    fn conflicts_with(&self, other: &LiveMemRef) -> bool {
        if !self.mutable && !other.mutable {
            // two shared slices never conflict
            return false
        }

        // Check for overlap
        (self.end > other.start) && (other.end > self.start)
    }
}

#[cfg(test)]
mod tests {
    use super::*;


    #[test]
    fn new() {
        let allocator = Allocator::new(Size(91));

        assert_eq!(allocator.allocations, vec![]);
        assert_eq!(allocator.free_by_addr, vec![Allocation::new(Address(0), Size(91))]);
        assert_eq!(allocator.free_by_size, vec![Allocation::new(Address(0), Size(91))]);
    }

    #[test]
    fn alloc_at_end() {
        let mut allocator = Allocator::new(Size(100));
        allocator.alloc(Size(10));

        assert_eq!(allocator.allocations, vec![Allocation::new(Address(0), Size(10))]);
        assert_eq!(allocator.free_by_addr, vec![Allocation::new(Address(10), Size(90))]);
        assert_eq!(allocator.free_by_size, vec![Allocation::new(Address(10), Size(90))]);
    }

    #[test]
    fn free_at_end() {
        let mut allocator = Allocator::new(Size(100));
        let alloc = allocator.alloc(Size(10));
        allocator.free(alloc);

        assert_eq!(allocator.allocations, vec![]);
        assert_eq!(allocator.free_by_addr, vec![Allocation::new(Address(0), Size(100))]);
        assert_eq!(allocator.free_by_size, vec![Allocation::new(Address(0), Size(100))]);
    }

    #[test]
    fn free_in_the_middle() {
        let mut allocator = Allocator::new(Size(100));
        allocator.alloc(Size(10));
        let alloc = allocator.alloc(Size(10));
        allocator.alloc(Size(10));
        allocator.free(alloc);

        assert_eq!(allocator.allocations, vec![Allocation::new(Address(0), Size(10)),
                                               Allocation::new(Address(20), Size(10))]);
        assert_eq!(allocator.free_by_addr, vec![Allocation::new(Address(10), Size(10)),
                                                Allocation::new(Address(30), Size(70))]);
        assert_eq!(allocator.free_by_size, vec![Allocation::new(Address(10), Size(10)),
                                                Allocation::new(Address(30), Size(70))]);
    }

    #[test]
    fn  merge_free_allocs_in_the_middle() {
        let mut allocator = Allocator::new(Size(100));
        allocator.alloc(Size(10));
        let alloc1 = allocator.alloc(Size(10));
        let alloc2 = allocator.alloc(Size(10));
        allocator.alloc(Size(10));
        allocator.free(alloc1);
        allocator.free(alloc2);

        assert_eq!(allocator.allocations, vec![Allocation::new(Address(0), Size(10)),
                                               Allocation::new(Address(30), Size(10))]);
        assert_eq!(allocator.free_by_addr, vec![Allocation::new(Address(10), Size(20)),
                                                Allocation::new(Address(40), Size(60))]);
        assert_eq!(allocator.free_by_size, vec![Allocation::new(Address(10), Size(20)),
                                                Allocation::new(Address(40), Size(60))]);
    }
}
