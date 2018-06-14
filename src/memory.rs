
use allocator::{Allocator, Allocation};
use std::ops::{Add, AddAssign, Sub, Mul};

#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct Address(pub u32);

#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct Size(pub u32);

pub trait Storage {
    fn size(&self) -> Size;
    fn write_bytes(&mut self, addr: Address, b: &[u8]);
    fn get_bytes(&self, addr: Address, len: Size) -> &[u8];
    fn get_bytes_mut(&mut self, addr: Address, len: Size) -> &mut [u8];
}

pub struct Memory<S: Storage> {
    storage: S,
    allocator: Allocator,
}

impl<S: Storage> Memory<S> {

    #[inline]
    pub fn new(storage: S) -> Memory<S> {
        Memory {
            allocator: Allocator::new(storage.size()),
            storage,
        }
    }

    #[inline]
    pub fn new_with_allocator(storage: S, allocator: Allocator) -> Memory<S> {
        assert!(storage.size() >= allocator.total_size());

        Memory {
            allocator,
            storage,
        }
    }

    #[inline]
    pub fn size(&self) -> Size {
        self.storage.size()
    }

    #[inline]
    pub fn write_bytes(&mut self, addr: Address, b: &[u8]) {
        self.storage.write_bytes(addr, b);
    }

    #[inline]
    pub fn get_bytes(&self, addr: Address, len: Size) -> &[u8] {
        self.storage.get_bytes(addr, len)
    }

    #[inline]
    pub fn get_bytes_mut(&mut self, addr: Address, len: Size) -> &mut [u8] {
        self.storage.get_bytes_mut(addr, len)
    }

    #[inline]
    pub fn alloc(&mut self, size: Size) -> Allocation {
        self.allocator.alloc(size)
    }

    #[inline]
    pub fn free(&mut self, allocation: Allocation) {
        for b in self.storage.get_bytes_mut(allocation.addr, allocation.size) {
            *b = 0;
        }
        self.allocator.free(allocation);
    }
}

pub struct MemStore {
    data: Vec<u8>,
}

impl MemStore {
    pub fn new(size: usize) -> MemStore {
        MemStore {
            data: vec![0u8; size],
        }
    }
}

impl Storage for MemStore {
    #[inline]
    fn size(&self) -> Size {
        Size::from_usize(self.data.len())
    }

    #[inline]
    fn write_bytes(&mut self, addr: Address, b: &[u8]) {
        let start = addr.0 as usize;
        let end = start + b.len();

        self.data[start .. end].copy_from_slice(b);
    }

    #[inline]
    fn get_bytes(&self, addr: Address, len: Size) -> &[u8] {
        &self.data[addr.0 as usize .. (addr + len).0 as usize]
    }

    #[inline]
    fn get_bytes_mut(&mut self, addr: Address, len: Size) -> &mut [u8] {
        &mut self.data[addr.0 as usize .. (addr + len).0 as usize]
    }
}



impl Add<Size> for Address {
    type Output = Address;

    #[inline]
    fn add(self, rhs: Size) -> Self::Output {
        Address(self.0 + rhs.0)
    }
}

impl Add<Size> for Size {
    type Output = Size;

    #[inline]
    fn add(self, rhs: Size) -> Self::Output {
        Size(self.0 + rhs.0)
    }
}

impl Sub<Size> for Size {
    type Output = Size;

    #[inline]
    fn sub(self, rhs: Size) -> Self::Output {
        Size(self.0 - rhs.0)
    }
}

impl Mul<Size> for Size {
    type Output = Size;

    #[inline]
    fn mul(self, rhs: Size) -> Self::Output {
        Size(self.0 * rhs.0)
    }
}

impl Mul<usize> for Size {
    type Output = Size;

    #[inline]
    fn mul(self, rhs: usize) -> Self::Output {
        Size(self.0 * rhs as u32)
    }
}

impl Mul<u32> for Size {
    type Output = Size;

    #[inline]
    fn mul(self, rhs: u32) -> Self::Output {
        Size(self.0 * rhs)
    }
}

impl AddAssign<Size> for Size {
    #[inline]
    fn add_assign(&mut self, rhs: Size) {
        self.0 += rhs.0;
    }
}

impl Address {
    #[inline]
    pub fn from_usize(x: usize) -> Address {
        let addr = Address(x as u32);
        assert!(addr.0 as usize == x);
        addr
    }

    #[inline]
    pub fn as_usize(self) -> usize {
        self.0 as usize
    }

    #[inline]
    pub fn from_u32(x: u32) -> Address {
        Address(x)
    }

    #[inline]
    pub fn as_u32(self) -> u32 {
        self.0
    }
}

impl Size {
    #[inline]
    pub fn from_usize(x: usize) -> Size {
        let size = Size(x as u32);
        assert!(size.0 as usize == x);
        size
    }

    #[inline]
    pub fn as_usize(self) -> usize {
        self.0 as usize
    }

    #[inline]
    pub fn from_u32(x: u32) -> Size {
        Size(x)
    }

    #[inline]
    pub fn as_u32(self) -> u32 {
        self.0
    }
}
