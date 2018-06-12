

use super::{Size, Address};

const MIN_ALLOC_SIZE: Size = Size(8);

#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct Allocation {
    pub addr: Address,
    pub size: Size,
}

impl Allocation {
    fn new(addr: Address, size: Size) -> Allocation {
        Allocation {
            addr,
            size,
        }
    }

    fn start(&self) -> Address {
        self.addr
    }

    fn end(&self) -> Address {
        self.addr + self.size
    }
}


pub struct Allocator {
    allocations: Vec<Allocation>,
    free_by_addr: Vec<Allocation>,
    free_by_size: Vec<Allocation>,
}


impl Allocator {

    pub fn new(total_size: Size) -> Allocator {
        Allocator {
            allocations: vec![],
            free_by_addr: vec![Allocation::new(Address(0), total_size)],
            free_by_size: vec![Allocation::new(Address(0), total_size)],
        }
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
                let available_alloc = self.free_by_size[index];
                assert!(available_alloc.size >= size);
                let remaining_space = available_alloc.size - size;

                if remaining_space < MIN_ALLOC_SIZE {
                    self.free_by_size.remove(index);
                    self.remove_free_by_addr(available_alloc);
                    self.insert_alloc(available_alloc);
                    available_alloc
                } else {
                    self.free_by_size.remove(index);
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
    }

    pub fn free(&mut self, addr: Address) {
        let freed_alloc = if let Ok(alloc_index) = self.find_alloc_by_address(addr) {
            self.allocations.remove(alloc_index)
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
        allocator.free(alloc.addr);

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
        allocator.free(alloc.addr);

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
        allocator.free(alloc1.addr);
        allocator.free(alloc2.addr);

        assert_eq!(allocator.allocations, vec![Allocation::new(Address(0), Size(10)),
                                               Allocation::new(Address(30), Size(10))]);
        assert_eq!(allocator.free_by_addr, vec![Allocation::new(Address(10), Size(20)),
                                                Allocation::new(Address(40), Size(60))]);
        assert_eq!(allocator.free_by_size, vec![Allocation::new(Address(10), Size(20)),
                                                Allocation::new(Address(40), Size(60))]);
    }
}
