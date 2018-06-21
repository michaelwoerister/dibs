
use std::mem;
use std::slice;
use std::cmp::Ordering;
use allocator::{Allocator, Allocation, LiveMemRef};
use std::ops::{Add, AddAssign, Sub, Mul, Div, Deref, DerefMut};
use persist::{Serialize, Deserialize, StorageWriter, StorageReader};
use parking_lot::Mutex;

#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct Address(pub u32);

#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct Size(pub u32);

#[derive(Clone)]
pub struct MemRef<'m> {
    slice: &'m [u8],

    #[cfg(debug_assertions)]
    allocator: &'m Mutex<Allocator>,
    #[cfg(debug_assertions)]
    mem_ref: LiveMemRef,
}

impl<'m1, 'm2> PartialEq<MemRef<'m1>> for MemRef<'m2> {
    fn eq(&self, other: &MemRef<'m1>) -> bool {
        self.slice == other.slice
    }
}

impl<'m> Eq for MemRef<'m> {}

impl<'m1, 'm2> PartialOrd<MemRef<'m1>> for MemRef<'m2> {
    fn partial_cmp(&self, other: &MemRef<'m1>) -> Option<Ordering> {
        (self.slice.as_ptr(), self.slice.len())
            .partial_cmp(&(other.slice.as_ptr(), other.slice.len()))
    }
}


impl<'m> Deref for MemRef<'m> {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        self.slice
    }
}

impl<'m> PartialEq<[u8]> for MemRef<'m> {
    fn eq(&self, other: &[u8]) -> bool {
        self.slice == other
    }
}

#[cfg(debug_assertions)]
impl<'m> Drop for MemRef<'m> {
    fn drop(&mut self) {
        self.allocator.lock().unregister_mem_ref(self.mem_ref);
    }
}

pub struct MemRefMut<'a> {
    slice: &'a mut [u8],

    #[cfg(debug_assertions)]
    allocator: &'a Mutex<Allocator>,
    #[cfg(debug_assertions)]
    mem_ref: LiveMemRef,
}

impl<'m, 'g> Deref for MemRefMut<'m> {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        self.slice
    }
}

impl<'m> DerefMut for MemRefMut<'m> {
    fn deref_mut(&mut self) -> &mut [u8] {
        self.slice
    }
}

impl<'m> PartialEq<[u8]> for MemRefMut<'m> {
    fn eq(&self, other: &[u8]) -> bool {
        self.slice == other
    }
}

#[cfg(debug_assertions)]
impl<'m> Drop for MemRefMut<'m> {
    fn drop(&mut self) {
        self.allocator.lock().unregister_mem_ref(self.mem_ref);
    }
}

pub trait Storage {
    const IS_READONLY: bool;
    fn size(&self) -> Size;
    unsafe fn get_bytes(&self, addr: Address, len: Size) -> &[u8];
    unsafe fn get_bytes_mut(&self, addr: Address, len: Size) -> &mut [u8];
    unsafe fn copy_nonoverlapping_exclusive(&mut self, src: Address, dst: Address, len: Size);
}

pub struct Memory<S: Storage> {
    pub(crate) storage: S,
    pub(crate) allocator: Mutex<Allocator>,
}

impl<S: Storage> Memory<S> {

    #[inline]
    pub fn new(storage: S) -> Memory<S> {
        Memory {
            allocator: Mutex::new(Allocator::new(storage.size())),
            storage,
        }
    }

    #[inline]
    pub fn new_with_allocator(storage: S, allocator: Allocator) -> Memory<S> {
        assert!(storage.size() >= allocator.total_size());

        Memory {
            allocator: Mutex::new(allocator),
            storage,
        }
    }

    #[inline]
    pub fn size(&self) -> Size {
        self.storage.size()
    }

    #[inline]
    pub fn get_bytes(&self, addr: Address, len: Size) -> MemRef {
        #[cfg(debug_assertions)]
        unsafe {
            MemRef {
                slice: self.storage.get_bytes(addr, len),
                allocator: &self.allocator,
                mem_ref: self.allocator.lock().register_mem_ref(addr, len, false),
            }
        }

        #[cfg(not(debug_assertions))]
        unsafe {
            MemRef {
                slice: self.storage.get_bytes(addr, len),
            }
        }
    }

    #[inline]
    pub fn get_bytes_mut(&self, addr: Address, len: Size) -> MemRefMut {
        assert!(!S::IS_READONLY);

        #[cfg(debug_assertions)]
        unsafe {
            MemRefMut {
                slice: self.storage.get_bytes_mut(addr, len),
                allocator: &self.allocator,
                mem_ref: self.allocator.lock().register_mem_ref(addr, len, true),
            }
        }

        #[cfg(not(debug_assertions))]
        unsafe {
            MemRefMut {
                slice: self.storage.get_bytes_mut(addr, len),
            }
        }
    }

    #[inline]
    pub fn alloc(&self, size: Size) -> Allocation {
        assert!(!S::IS_READONLY);

        self.allocator.lock().alloc(size)
    }

    #[inline]
    pub fn free(&self, allocation: Allocation) {
        assert!(!S::IS_READONLY);

        unsafe {
            fill_zero(&mut self.storage.get_bytes_mut(allocation.addr, allocation.size));
        }
        self.allocator.lock().free(allocation);
    }

    #[inline]
    pub fn copy_nonoverlapping(&self, src: Address, dst: Address, len: Size) {
        assert!(!S::IS_READONLY);

        let src_end = src + len;
        let dst_end = dst + len;

        assert!(src >= dst_end || dst >= src_end);

        self.get_bytes_mut(dst, len).copy_from_slice(&self.get_bytes(src, len));
    }
}

// impl<S: Storage> Storage for Memory<S> {
//     #[inline]
//     fn size(&self) -> Size {
//         self.storage.size()
//     }

//     #[inline]
//     fn get_bytes(&self, addr: Address, len: Size) -> MemRef {
//         self.storage.get_bytes(addr, len)
//     }

//     fn get_bytes_mut(&self, addr: Address, len: Size) -> MemRefMut {
//         self.storage.get_bytes_mut(addr, len)
//     }

//     #[inline]
//     fn get_bytes_mut_exclusive(&mut self, addr: Address, len: Size) -> &mut [u8] {
//         self.storage.get_bytes_mut_exclusive(addr, len)
//     }

//     #[inline]
//     fn copy_nonoverlapping_exclusive(&mut self, src: Address, dst: Address, len: Size) {
//         self.storage.copy_nonoverlapping_exclusive(src, dst, len);
//     }
// }

pub struct MemStore {
    data: *mut u8,
    len: usize,
    // used for dropping
    capacity: usize,
}

impl MemStore {
    pub fn new(size: usize) -> MemStore {
        let mut vec = vec![0u8; size];

        let data = vec.as_mut_ptr();
        let len = vec.len();
        let capacity = vec.capacity();

        mem::forget(vec);

        MemStore {
            data,
            len,
            capacity,
        }
    }

    fn get_slice(&self, start: Address, len: Size) -> &[u8] {
        assert!((start + len).as_usize() <= self.len);

        unsafe {
            slice::from_raw_parts(self.data.offset(start.as_isize()), len.as_usize())
        }
    }

    fn get_slice_mut(&self, start: Address, len: Size) -> &mut [u8] {
        assert!((start + len).as_usize() <= self.len);

        unsafe {
            slice::from_raw_parts_mut(self.data.offset(start.as_isize()), len.as_usize())
        }
    }
}

impl Drop for MemStore {
    fn drop(&mut self) {
        let drop_me = unsafe {
            Vec::from_raw_parts(self.data, self.len, self.capacity)
        };
        mem::drop(drop_me);
    }
}

impl Storage for MemStore {
    const IS_READONLY: bool = false;

    #[inline]
    fn size(&self) -> Size {
        Size::from_usize(self.len)
    }

    #[inline]
    unsafe fn get_bytes(&self, addr: Address, len: Size) -> &[u8] {
        self.get_slice(addr, len)
    }

    unsafe fn get_bytes_mut(&self, addr: Address, len: Size) -> &mut [u8] {
        self.get_slice_mut(addr, len)
    }

    #[inline]
    unsafe fn copy_nonoverlapping_exclusive(&mut self, src: Address, dst: Address, len: Size) {
        #[cfg(debug_assertions)]
        {
            // TODO assert non-overlapping
        }

        self.get_slice_mut(dst, len).copy_from_slice(self.get_slice(src, len));
    }
}



impl Add<Size> for Address {
    type Output = Address;

    #[inline]
    fn add(self, rhs: Size) -> Self::Output {
        Address(self.0 + rhs.0)
    }
}

impl AddAssign<Size> for Address {
    #[inline]
    fn add_assign(&mut self, rhs: Size) {
        self.0 += rhs.0;
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

impl Div<u32> for Size {
    type Output = Size;

    #[inline]
    fn div(self, rhs: u32) -> Self::Output {
        Size(self.0 / rhs)
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
    pub fn as_isize(self) -> isize {
        self.0 as isize
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

impl Serialize for Address {
    #[inline]
    fn write<'s, S: Storage + 's>(&self, writer: &mut StorageWriter<'s, S>) {
        writer.write_u32(self.0);
    }
}

impl Deserialize for Address {
    #[inline]
    fn read<'s, S: Storage + 's>(reader: &mut StorageReader<'s, S>) -> Address {
        Address(reader.read_u32())
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

impl Serialize for Size {
    #[inline]
    fn write<'s, S: Storage + 's>(&self, writer: &mut StorageWriter<'s, S>) {
        writer.write_u32(self.0);
    }
}

impl Deserialize for Size {
    #[inline]
    fn read<'s, S: Storage + 's>(reader: &mut StorageReader<'s, S>) -> Size {
        Size(reader.read_u32())
    }
}

#[inline]
pub fn fill_zero(slice: &mut [u8]) {
    for b in slice {
        *b = 0;
    }
}
