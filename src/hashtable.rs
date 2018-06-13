
use std::mem;
use byteorder::{LittleEndian, ByteOrder};
use allocator::Allocation;
use {Storage, Address, Size, Memory};

const LEN_OFFSET: Size = Size(0);
const CAPACITY_OFFSET: Size = Size(mem::size_of::<Size>() as u32);

const HEADER_SIZE: Size = Size(2 * mem::size_of::<Size>() as u32);
const ENTRY_SIZE: Size = Size(mem::size_of::<Entry>() as u32);

const EMPTY_ENTRY: Address = Address(0);

pub struct HashTable<'m, S: Storage + 'm> {
    data: Allocation,
    memory: &'m mut Memory<S>,
}

impl<'m, S: Storage + 'm> HashTable<'m, S> {
    #[inline]
    pub fn new(memory: &'m mut Memory<S>) -> HashTable<'m, S> {
        HashTable::with_capacity(memory, Size(0))
    }

    pub fn with_capacity(memory: &'m mut Memory<S>, capacity: Size) -> HashTable<'m, S> {
        let data = Self::alloc_with_capacity(memory, capacity);

        HashTable {
            data,
            memory,
        }
    }

    fn alloc_with_capacity(memory: &'m mut Memory<S>, capacity: Size) -> Allocation {
        let byte_count = byte_count_for_capacity(capacity);
        let data = memory.alloc(byte_count);

        Self::set_len_raw(memory, data, Size(0));
        Self::set_capacity_raw(memory, data, capacity);

        data
    }

    #[inline]
    fn set_len_raw(memory: &'m mut Memory<S>, data: Allocation, len: Size) {
        let addr = data.addr + LEN_OFFSET;
        LittleEndian::write_u32(memory.get_bytes_mut(addr, Size(4)), len.0);
    }

    #[inline]
    fn set_capacity_raw(memory: &'m mut Memory<S>, data: Allocation, capacity: Size) {
        let addr = data.addr + CAPACITY_OFFSET;
        LittleEndian::write_u32(memory.get_bytes_mut(addr, Size(4)), capacity.0);
    }

    #[inline]
    pub fn len(&self) -> usize {
        let addr = self.data.addr + LEN_OFFSET;
        LittleEndian::read_u32(self.memory.get_bytes(addr, Size(4))) as usize
    }

    #[inline]
    pub fn capacity(&self) -> usize {
        let addr = self.data.addr + CAPACITY_OFFSET;
        LittleEndian::read_u32(self.memory.get_bytes(addr, Size(4))) as usize
    }

    pub fn find(&self, key: &[u8]) -> Option<&[u8]> {
        let table_size = Self::table_size(self.data);
        let hash = hash_for(key);
        let mut index = hash % table_size;

        loop {
            let entry = self.get_entry(index);

            if entry.key == EMPTY_ENTRY {
                return None
            } else if self.load_data(entry.key) == key {
                return Some(self.load_data(entry.value))
            }

            index = advance_index(index, table_size);
        }
    }

    pub fn insert(&mut self, key: &[u8], value: &[u8]) -> bool {
        if self.len() >= self.capacity() {
            let new_capacity = if self.capacity() == 0 {
                8
            } else {
                (self.capacity() * 3) / 2
            };
            self.resize(Size::from_usize(new_capacity));
        }

        let table_size = Self::table_size(self.data);
        let hash = hash_for(key);
        let mut index = hash % table_size;

        for _ in 0 .. table_size {
            let entry = self.get_entry(index);

            if entry.key == EMPTY_ENTRY {
                let new_entry = Entry {
                    key: self.store_data(key),
                    value: self.store_data(value),
                    hash,
                };
                self.set_entry(index, new_entry);

                debug_assert_eq!(self.get_entry(index), new_entry);

                let old_len = Size::from_usize(self.len());
                Self::set_len_raw(self.memory, self.data, old_len + Size(1));
                debug_assert_eq!(Size::from_usize(self.len()), old_len + Size(1));

                self.sanity_check_table();

                return true
            }

            if self.load_data(entry.key) == key {
                debug_assert_eq!(hash, entry.hash);
                self.delete_data(entry.value);

                let value = self.store_data(value);

                self.set_entry(index, Entry {
                    key: entry.key,
                    value,
                    hash,
                });

                self.sanity_check_table();

                return false
            }

            index = advance_index(index, table_size);
        }

        unreachable!("No free entry found?")
    }

    pub fn remove(&mut self, key: &[u8]) -> bool {
        if self.len() == 0 {
            return false
        }

        let table_size = Self::table_size(self.data);
        let hash = hash_for(key);
        let mut index = hash % table_size;

        loop {
            let entry = self.get_entry(index);

            if entry.key == EMPTY_ENTRY {
                return false
            } else if self.load_data(entry.key) == key {
                debug_assert_eq!(hash, entry.hash);
                self.delete_data(entry.key);
                self.delete_data(entry.value);

                self.set_entry(index, Entry {
                    key: EMPTY_ENTRY,
                    value: EMPTY_ENTRY,
                    hash: 0,
                });

                self.repair_block_after_deletion(index);

                let old_len = Size::from_usize(self.len());
                Self::set_len_raw(self.memory, self.data, old_len - Size(1));

                self.sanity_check_table();

                return true
            }

            index = advance_index(index, table_size);
        }
    }

    fn repair_block_after_deletion(&mut self, deletion_index: u32) {
        let table_size = Self::table_size(self.data);
        let mut search_index = advance_index(deletion_index, table_size);

        loop {
            let entry = self.get_entry(search_index);

            if entry.key == EMPTY_ENTRY {
                // nothing to do
                return
            }

            let min_entry_index = entry.hash % table_size;

            if search_index > min_entry_index {
                if deletion_index >= min_entry_index && deletion_index < search_index {
                    // replace
                    self.set_entry(deletion_index, entry);
                    self.set_entry(search_index, Entry {
                        key: EMPTY_ENTRY,
                        value: EMPTY_ENTRY,
                        hash: 0,
                    });

                    self.repair_block_after_deletion(search_index);
                    return
                }
            } else if search_index < min_entry_index {
                if deletion_index >= min_entry_index || deletion_index < search_index {
                    // replace
                    self.set_entry(deletion_index, entry);
                    self.set_entry(search_index, Entry {
                        key: EMPTY_ENTRY,
                        value: EMPTY_ENTRY,
                        hash: 0,
                    });

                    self.repair_block_after_deletion(search_index);
                    return
                }
            } else {
                // The entry at search index is already at its optimal position,
                // so we can't ever move it.
            }

            search_index = advance_index(search_index, table_size);
        }
    }

    fn table_size(data: Allocation) -> u32 {
        debug_assert_eq!((data.size - HEADER_SIZE).0 % ENTRY_SIZE.0, 0);
        (data.size - HEADER_SIZE).0 / ENTRY_SIZE.0
    }

    #[inline]
    fn get_entry(&self, index: u32) -> Entry {
        Self::get_entry_raw(self.memory, self.data, index)
    }

    fn get_entry_raw(memory: &'m Memory<S>, data: Allocation, index: u32) -> Entry {
        let addr = data.addr + HEADER_SIZE + ENTRY_SIZE * index;
        let bytes = memory.get_bytes(addr, ENTRY_SIZE);

        Entry {
            key: Address(LittleEndian::read_u32(bytes)),
            value: Address(LittleEndian::read_u32(&bytes[4 ..])),
            hash: LittleEndian::read_u32(&bytes[8 ..]),
        }
    }

    #[inline]
    fn set_entry(&mut self, index: u32, entry: Entry) {
        Self::set_entry_raw(self.memory, self.data, index, entry)
    }

    fn set_entry_raw(memory: &'m mut Memory<S>, data: Allocation, index: u32, entry: Entry) {
        let addr = data.addr + HEADER_SIZE + ENTRY_SIZE * index;
        let bytes = memory.get_bytes_mut(addr, ENTRY_SIZE);

        LittleEndian::write_u32(bytes, entry.key.0);
        LittleEndian::write_u32(&mut bytes[4 ..], entry.value.0);
        LittleEndian::write_u32(&mut bytes[8 ..], entry.hash);
    }

    fn resize(&mut self, new_capacity: Size) {
        let new_table_data = Self::alloc_with_capacity(self.memory, new_capacity);
        let new_table_size = Self::table_size(new_table_data);
        let len = self.len();

        let mut written = 0;

        'outer: for read_index in 0 .. Self::table_size(self.data) {
            let entry = self.get_entry(read_index);

            if entry.key == EMPTY_ENTRY {
                // Empty entry, nothing to copy
                continue
            }

            let mut insertion_index = entry.hash % new_table_size;

            for _ in 0 .. new_table_size {
                let new_entry = Self::get_entry_raw(self.memory, new_table_data, insertion_index);

                if new_entry.key == EMPTY_ENTRY {
                    Self::set_entry_raw(self.memory, new_table_data, insertion_index, entry);
                    debug_assert_eq!(Self::get_entry_raw(self.memory, new_table_data, insertion_index), entry);

                    written += 1;
                    debug_assert!(written <= len,
                        "more non-null entries than len() in table. \
                         written = {}, len={}", written, len);
                    continue 'outer
                }

                insertion_index = advance_index(insertion_index, new_table_size);
            }

            panic!("no free entry found? len={}, old_capacity={}, \
                    old_table_size={}, new_capacity={}, new_table_size={}",
                self.len(),
                self.capacity(),
                Self::table_size(self.data),
                new_capacity.0,
                new_table_size);
        }

        debug_assert_eq!(written, len);
        Self::set_len_raw(self.memory, new_table_data, Size::from_usize(len));

        self.memory.free(self.data);
        self.data = new_table_data;
    }

    fn sanity_check_entry(&self, index: u32) {
        let entry = self.get_entry(index);
        if entry.key == EMPTY_ENTRY {
            return
        }

        let table_size = Self::table_size(self.data);

        let min_entry_index = entry.hash % table_size;

        let mut i = index;
        while i != min_entry_index {
            assert!(self.get_entry(i).key != EMPTY_ENTRY,
            "table_size = {}, index = {}, min_entry_index={}, i={}",
            table_size,
            index,
            min_entry_index,
            i);

            if i == 0 {
                i = table_size - 1;
            } else {
                i -= 1;
            }
        }
    }

    pub fn sanity_check_table(&self) {
        for index in 0 .. Self::table_size(self.data) {
            self.sanity_check_entry(index);
        }
    }

    pub fn iter<F: FnMut(&[u8], &[u8])>(&self, mut f: F) {
        let table_size = Self::table_size(self.data);
        for index in 0 .. table_size {
            let entry = self.get_entry(index);

            if entry.key != EMPTY_ENTRY {
                f(self.load_data(entry.key), self.load_data(entry.value));
            }
        }
    }

    fn store_data(&mut self, key: &[u8]) -> Address {
        assert!(key.len() < 256);

        let allocation = self.memory.alloc(Size::from_usize(key.len() + 1));
        self.memory.write_bytes(allocation.addr, &[key.len() as u8]);
        self.memory.write_bytes(allocation.addr + Size(1), key);
        allocation.addr
    }

    fn load_data(&self, addr: Address) -> &[u8] {
        let len = self.memory.get_bytes(addr, Size(1))[0] as u32;
        self.memory.get_bytes(addr + Size(1), Size(len))
    }

    fn delete_data(&mut self, addr: Address) {
        let len = self.memory.get_bytes(addr, Size(1))[0] as u32;
        self.memory.free(Allocation {
            addr,
            size: Size(len + 1),
        });
    }
}

#[derive(Copy, Clone, Eq, PartialEq, Debug)]
struct Entry {
    key: Address,
    value: Address,
    hash: u32,
}

#[inline]
fn byte_count_for_capacity(capacity: Size) -> Size {
    HEADER_SIZE + ENTRY_SIZE * capacity * 2u32
}

#[inline]
fn hash_for(key: &[u8]) -> u32 {
    use metrohash::MetroHash;
    use std::hash::Hasher;
    let mut hasher = MetroHash::default();
    hasher.write(key);
    hasher.finish() as u32
}

#[inline]
fn advance_index(index: u32, table_size: u32) -> u32 {
    debug_assert!(index < table_size);
    (index + 1) % table_size
}


#[cfg(test)]
mod tests {
    use super::*;
    use MemStore;

    fn create_memory(size: usize) -> Memory<MemStore> {
        let mut memory = Memory::new(MemStore::new(size));

        memory.alloc(Size(1));

        memory
    }

    #[test]
    fn test_new() {
        let mut memory = create_memory(100);
        let hash_table = HashTable::new(&mut memory);
        assert_eq!(hash_table.len(), 0);
        assert_eq!(hash_table.capacity(), 0);

        hash_table.sanity_check_table();
    }

    #[test]
    fn test_with_capacity() {
        let mut memory = create_memory(10000);
        let hash_table = HashTable::with_capacity(&mut memory, Size(100));
        assert_eq!(hash_table.len(), 0);
        assert_eq!(hash_table.capacity(), 100);

        hash_table.sanity_check_table();
    }
}
