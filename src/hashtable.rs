
use std::mem;
use byteorder::{LittleEndian, ByteOrder};
use allocator::Allocator;
use {Storage, Address, Size};

const LEN_OFFSET: usize = 0;
const CAPACITY_OFFSET: usize = mem::size_of::<Size>();

const HEADER_SIZE: usize = 2 * mem::size_of::<Size>();
const ENTRY_SIZE: usize = mem::size_of::<Entry>();

const EMPTY_ENTRY: Address = Address(0);

pub struct HashTable {
    bytes: Vec<u8>,
}

impl HashTable {
    #[inline]
    pub fn new() -> HashTable {
        HashTable::with_capacity(0)
    }

    pub fn with_capacity(capacity: usize) -> HashTable {
        let byte_count = byte_count_for_capacity(capacity);
        let mut bytes = vec![0; byte_count];

        LittleEndian::write_u32(&mut bytes[LEN_OFFSET ..], Size(0).0);
        LittleEndian::write_u32(&mut bytes[CAPACITY_OFFSET ..], Size::from_usize(capacity).0);

        HashTable {
            bytes
        }
    }

    #[inline]
    fn set_len(&mut self, len: Size) {
         LittleEndian::write_u32(&mut self.bytes[LEN_OFFSET ..], len.0);
    }

    #[inline]
    pub fn len(&self) -> usize {
        LittleEndian::read_u32(&self.bytes[LEN_OFFSET..]) as usize
    }

    #[inline]
    pub fn capacity(&self) -> usize {
        LittleEndian::read_u32(&self.bytes[CAPACITY_OFFSET..]) as usize
    }

    #[inline]
    pub fn read_view(&self) -> ReadView {
        ReadView {
            bytes: &self.bytes[..]
        }
    }

    pub fn find<'storage, S: Storage>(&self,
                                      key: &[u8],
                                      storage: &'storage S)
                                      -> Option<&'storage [u8]> {
        let table_size = self.table_size();
        let hash = hash_for(key);
        let mut index = hash % table_size;

        loop {
            let entry = self.get_entry(index);

            if entry.key == EMPTY_ENTRY {
                return None
            } else if load_data(storage, entry.key) == key {
                return Some(load_data(storage, entry.value))
            }

            index = advance_index(index, table_size);
        }
    }

    pub fn insert<S: Storage>(&mut self,
                              key: &[u8],
                              value: &[u8],
                              storage: &mut S,
                              allocator: &mut Allocator) -> bool {
        if self.len() >= self.capacity() {
            let new_capacity = if self.capacity() == 0 {
                8
            } else {
                (self.capacity() as u32 * 3) / 2
            };
            self.resize(new_capacity);
        }

        let table_size = self.table_size();
        let hash = hash_for(key);
        let mut index = hash % table_size;

        for _ in 0 .. table_size {
            let entry = self.get_entry(index);

            if entry.key == EMPTY_ENTRY {
                let new_entry = Entry {
                    key: store_data(storage, allocator, key),
                    value: store_data(storage, allocator, value),
                    hash,
                };
                self.set_entry(index, new_entry);

                debug_assert_eq!(self.get_entry(index), new_entry);

                let old_len = Size::from_usize(self.len());
                self.set_len(old_len + Size(1));
                debug_assert_eq!(Size::from_usize(self.len()), old_len + Size(1));

                self.sanity_check_table();

                return true
            }

            if load_data(storage, entry.key) == key {
                debug_assert_eq!(hash, entry.hash);
                delete_data(storage, allocator, entry.value);

                self.set_entry(index, Entry {
                    key: entry.key,
                    value: store_data(storage, allocator, value),
                    hash,
                });

                self.sanity_check_table();

                return false
            }

            index = advance_index(index, table_size);
        }

        unreachable!("No free entry found?")
    }

    pub fn remove<S: Storage>(&mut self,
                              key: &[u8],
                              storage: &mut S,
                              allocator: &mut Allocator) -> bool {
        if self.len() == 0 {
            return false
        }

        let table_size = self.table_size();
        let hash = hash_for(key);
        let mut index = hash % table_size;

        loop {
            let entry = self.get_entry(index);

            if entry.key == EMPTY_ENTRY {
                return false
            } else if load_data(storage, entry.key) == key {
                debug_assert_eq!(hash, entry.hash);
                delete_data(storage, allocator, entry.key);
                delete_data(storage, allocator, entry.value);

                self.set_entry(index, Entry {
                    key: EMPTY_ENTRY,
                    value: EMPTY_ENTRY,
                    hash: 0,
                });

                self.repair_block_after_deletion(index);

                let old_len = Size::from_usize(self.len());
                self.set_len(old_len - Size(1));

                self.sanity_check_table();

                return true
            }

            index = advance_index(index, table_size);
        }
    }

    fn repair_block_after_deletion(&mut self, deletion_index: u32) {
        let table_size = self.table_size();
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

    fn table_size(&self) -> u32 {
        debug_assert_eq!((self.bytes.len() - HEADER_SIZE) % ENTRY_SIZE, 0);
        ((self.bytes.len() - HEADER_SIZE) / ENTRY_SIZE) as u32
    }

    fn get_entry(&self, index: u32) -> Entry {
        let byte_offset = HEADER_SIZE + index as usize * ENTRY_SIZE;
        let bytes = &self.bytes[byte_offset .. byte_offset + ENTRY_SIZE];

        Entry {
            key: Address(LittleEndian::read_u32(bytes)),
            value: Address(LittleEndian::read_u32(&bytes[4 ..])),
            hash: LittleEndian::read_u32(&bytes[8 ..]),
        }
    }

    fn set_entry(&mut self, index: u32, entry: Entry) {
        let byte_offset = HEADER_SIZE + index as usize * ENTRY_SIZE;
        let bytes = &mut self.bytes[byte_offset .. byte_offset + ENTRY_SIZE];

        LittleEndian::write_u32(bytes, entry.key.0);
        LittleEndian::write_u32(&mut bytes[4 ..], entry.value.0);
        LittleEndian::write_u32(&mut bytes[8 ..], entry.hash);
    }

    fn resize(&mut self, new_capacity: u32) {
        let mut new_table = HashTable::with_capacity(new_capacity as usize);
        let new_table_size = new_table.table_size();

        let mut written = 0;

        'outer: for read_index in 0 .. self.table_size() {
            let entry = self.get_entry(read_index);

            if entry.key == EMPTY_ENTRY {
                // Empty entry, nothing to copy
                continue
            }

            let mut insertion_index = entry.hash % new_table_size;

            for _ in 0 .. new_table_size {
                let new_entry = new_table.get_entry(insertion_index);

                if new_entry.key == EMPTY_ENTRY {
                    new_table.set_entry(insertion_index, entry);
                    debug_assert_eq!(new_table.get_entry(insertion_index), entry);

                    written += 1;
                    debug_assert!(written <= self.len(),
                        "more non-null entries than len() in table. \
                         written = {}, len={}", written, self.len());
                    continue 'outer
                }

                insertion_index = advance_index(insertion_index, new_table_size);
            }

            panic!("no free entry found? len={}, old_capacity={}, \
                    old_table_size={}, new_capacity={}, new_table_size={}",
                self.len(),
                self.capacity(),
                self.table_size(),
                new_table.capacity(),
                new_table_size);
        }

        debug_assert_eq!(written, self.len());
        new_table.set_len(Size::from_usize(self.len()));

        *self = new_table;
    }

    fn sanity_check_entry(&self, index: u32) {
        let entry = self.get_entry(index);
        if entry.key == EMPTY_ENTRY {
            return
        }

        let table_size = self.table_size();

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
        for index in 0 .. self.table_size() {
            self.sanity_check_entry(index);
        }
    }

    pub fn iter<S: Storage, F: FnMut(&[u8], &[u8])>(&self, storage: &S, mut f: F) {
        let table_size = self.table_size();
        for index in 0 .. table_size {
            let entry = self.get_entry(index);

            if entry.key != EMPTY_ENTRY {
                f(load_data(storage, entry.key), load_data(storage, entry.value));
            }
        }
    }
}

#[derive(Copy, Clone, Eq, PartialEq, Debug)]
struct Entry {
    key: Address,
    value: Address,
    hash: u32,
}

#[inline]
fn byte_count_for_capacity(capacity: usize) -> usize {
    HEADER_SIZE + ENTRY_SIZE * capacity * 2
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

fn store_data<S: Storage>(storage: &mut S, allocator: &mut Allocator, key: &[u8]) -> Address {
    assert!(key.len() < 256);

    let allocation = allocator.alloc(Size::from_usize(key.len() + 1));
    storage.write_bytes(allocation.addr, &[key.len() as u8]);
    storage.write_bytes(allocation.addr + Size(1), key);
    allocation.addr
}

fn load_data<S: Storage>(storage: &S, addr: Address) -> &[u8] {
    let len = storage.get_bytes(addr, Size(1))[0] as u32;
    storage.get_bytes(addr + Size(1), Size(len))
}

fn delete_data<S: Storage>(_storage: &mut S,
                           allocator: &mut Allocator,
                           addr: Address) {
    allocator.free(addr);
}

pub struct ReadView<'a> {
    bytes: &'a [u8],
}

impl<'a> ReadView<'a> {

    #[inline]
    pub fn len(&self) -> usize {
        LittleEndian::read_u32(&self.bytes[LEN_OFFSET..]) as usize
    }

    #[inline]
    pub fn capacity(&self) -> usize {
        LittleEndian::read_u32(&self.bytes[CAPACITY_OFFSET..]) as usize
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new() {
        let hash_table = HashTable::new();
        assert_eq!(hash_table.len(), 0);
        assert_eq!(hash_table.read_view().len(), 0);

        assert_eq!(hash_table.capacity(), 0);
        assert_eq!(hash_table.read_view().capacity(), 0);

        hash_table.sanity_check_table();
    }

    #[test]
    fn test_with_capacity() {
        let hash_table = HashTable::with_capacity(100);
        assert_eq!(hash_table.len(), 0);
        assert_eq!(hash_table.read_view().len(), 0);

        assert_eq!(hash_table.capacity(), 100);
        assert_eq!(hash_table.read_view().capacity(), 100);

        hash_table.sanity_check_table();
    }
}
