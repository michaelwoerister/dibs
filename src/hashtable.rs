
use std::mem;
use byteorder::{LittleEndian, ByteOrder};
use allocator::Allocation;
use persist::*;
use memory::*;

const MAGIC_HEADER: [u8; 4] = [b'H', b'A', b'S', b'H'];

const MAGIC_HEADER_OFFSET: Size = Size(0);
const LEN_OFFSET: Size = Size(MAGIC_HEADER_OFFSET.0 + 4);
const CAPACITY_OFFSET: Size = Size(LEN_OFFSET.0 + 4);

const HEADER_SIZE: Size = Size(CAPACITY_OFFSET.0 + 4);
const ENTRY_META_SIZE: Size = Size(8);

// TODO: support deleting table

// Layout:
//
// magic_header: u32
// item_count: u32
// capacity: u32
// entry*
pub struct HashTable<'m, S: Storage + 'm, C: HashTableConfig = DefaultHashTableConfig> {
    data: Allocation,
    memory: &'m mut Memory<S>,
    config: ::std::marker::PhantomData<C>,
}

pub trait HashTableConfig {
    const MAX_INLINE_KEY_LEN: Size = Size(4);
    const MAX_INLINE_VALUE_LEN: Size = Size(4);
}

pub enum DefaultHashTableConfig {}
impl HashTableConfig for DefaultHashTableConfig {}

const ENTRY_META_IS_EMPTY_BIT: u64 = 1 << 63;
// const ENTRY_META_IS_TOMBSTONE_BIT: u64 = 1 << 62;
const ENTRY_META_INLINE_LEN_BIT_COUNT: usize = 7;
const ENTRY_META_INLINE_LEN_MASK: u64 = (1u64 << ENTRY_META_INLINE_LEN_BIT_COUNT) - 1;
const ENTRY_META_HASH_BIT_COUNT: usize = 64 - (4 + ENTRY_META_INLINE_LEN_BIT_COUNT * 2);
const ENTRY_META_HASH_MASK: u64 = (1u64 << ENTRY_META_HASH_BIT_COUNT) - 1;


trait EntryDataKind {
    const IS_INLINE_BIT_SHIFT: usize;
    const IS_INLINE_BIT: u64 = 1 << Self::IS_INLINE_BIT_SHIFT;
    const IS_INLINE_CLEAR_MASK: u64 = !Self::IS_INLINE_BIT;

    const INLINE_LEN_SHIFT: usize;
    const INLINE_LEN_CLEAR_MASK: u64 = !(ENTRY_META_INLINE_LEN_MASK << Self::INLINE_LEN_SHIFT);

    fn max_inline_size<C: HashTableConfig>() -> Size;
    fn offset_within_entry<C: HashTableConfig>() -> Size;
}

enum DataKindKey {}
impl EntryDataKind for DataKindKey {
    const IS_INLINE_BIT_SHIFT: usize = 61;
    const INLINE_LEN_SHIFT: usize = ENTRY_META_HASH_BIT_COUNT;

    fn max_inline_size<C: HashTableConfig>() -> Size {
        C::MAX_INLINE_KEY_LEN
    }

    fn offset_within_entry<C: HashTableConfig>() -> Size {
        ENTRY_META_SIZE
    }
}

enum DataKindValue {}
impl EntryDataKind for DataKindValue {
    const IS_INLINE_BIT_SHIFT: usize = 60;
    const INLINE_LEN_SHIFT: usize = ENTRY_META_HASH_BIT_COUNT + ENTRY_META_INLINE_LEN_BIT_COUNT;

    fn max_inline_size<C: HashTableConfig>() -> Size {
        C::MAX_INLINE_VALUE_LEN
    }

    fn offset_within_entry<C: HashTableConfig>() -> Size {
        ENTRY_META_SIZE + C::MAX_INLINE_KEY_LEN
    }
}

#[derive(Copy, Clone, Eq, PartialEq)]
struct Entry<C: HashTableConfig, S: Storage> {
    metadata: u64,
    addr: Address,
    config: ::std::marker::PhantomData<C>,
    storage: ::std::marker::PhantomData<S>,
}

impl<C: HashTableConfig, S: Storage> Entry<C, S> {

    fn init_non_empty(&mut self, storage: &mut Memory<S>, hash: u64) {
        self.metadata = (hash & ENTRY_META_HASH_MASK) | ENTRY_META_IS_EMPTY_BIT;
        self.metadata.write_at(storage, self.addr);
        debug_assert!(!self.is_empty());
    }

    fn clear(&mut self, memory: &mut Memory<S>) {
        debug_assert!(!self.is_empty());
        self.delete_entry_data::<DataKindKey>(memory);
        self.delete_entry_data::<DataKindValue>(memory);
        fill_zero(memory.get_bytes_mut(self.addr, HashTable::<S, C>::ENTRY_SIZE));
        self.metadata = 0;
        debug_assert!(self.is_empty());
    }

    #[inline]
    fn hash(&self) -> u64 {
        self.metadata & ENTRY_META_HASH_MASK
    }

    fn hash_equal(&self, hash: u64) -> bool {
        self.hash() == (hash & ENTRY_META_HASH_MASK)
    }

    fn is_empty(&self) -> bool {
        (self.metadata & ENTRY_META_IS_EMPTY_BIT) == 0
    }

    // fn is_tombstone(self) -> bool {
    //     (self.0 & ENTRY_META_IS_TOMBSTONE_BIT) != 0
    // }

    fn is_entry_data_inline<K: EntryDataKind>(&self) -> bool {
        (self.metadata & K::IS_INLINE_BIT) == 0
    }

    fn inline_entry_data_len<K: EntryDataKind>(&self) -> Size {
        Size(((self.metadata >> K::INLINE_LEN_SHIFT) & ENTRY_META_INLINE_LEN_MASK) as u32)
    }

    fn entry_data<'m, K: EntryDataKind>(&self, memory: &'m Memory<S>, ) -> &'m [u8] {
        let data_addr = self.addr + K::offset_within_entry::<C>();

        if self.is_entry_data_inline::<K>() {
            let inline_data_len = self.inline_entry_data_len::<K>();
            memory.get_bytes(data_addr, inline_data_len)
        } else {
            // Follow the indirection
            let data_addr = Address::read_at(memory, data_addr);
            let len = Size(memory.get_bytes(data_addr, Size(1))[0] as u32);
            memory.get_bytes(data_addr + Size(1), len)
        }
    }

    fn set_entry_data<K: EntryDataKind>(&mut self,
                                            memory: &mut Memory<S>,
                                            bytes: &[u8]) {
        assert!(bytes.len() < 256);

        self.delete_entry_data::<K>(memory);

        let max_inline_size = K::max_inline_size::<C>();
        debug_assert!(!self.is_empty());

        if bytes.len() <= max_inline_size.as_usize() {
            {
                let dest_bytes = memory.get_bytes_mut(self.addr + K::offset_within_entry::<C>(),
                                                      max_inline_size);
                dest_bytes[0 .. bytes.len()].copy_from_slice(bytes);
                fill_zero(&mut dest_bytes[bytes.len() .. ]);
            }

            self.metadata &= K::IS_INLINE_CLEAR_MASK;
            self.metadata &= K::INLINE_LEN_CLEAR_MASK;
            self.metadata |= (bytes.len() as u64) << K::INLINE_LEN_SHIFT;

            debug_assert_eq!(self.is_entry_data_inline::<K>(), true);
            debug_assert_eq!(self.inline_entry_data_len::<K>(), Size::from_usize(bytes.len()));
        } else {
            let addr = {
                let allocation = memory.alloc(Size::from_usize(bytes.len() + 1));
                let dest_bytes = memory.get_bytes_mut(allocation.addr, allocation.size);
                dest_bytes[0] = bytes.len() as u8;
                dest_bytes[1 ..].copy_from_slice(bytes);
                allocation.addr
            };

            const ADDRESS_SIZE: usize = mem::size_of::<Address>();
            let dest_bytes = memory.get_bytes_mut(self.addr + K::offset_within_entry::<C>(),
                                                  max_inline_size);
            LittleEndian::write_u32(&mut dest_bytes[0 .. ADDRESS_SIZE], addr.as_u32());
            fill_zero(&mut dest_bytes[ADDRESS_SIZE .. ]);

            self.metadata |= K::IS_INLINE_BIT;
            self.metadata &= K::INLINE_LEN_CLEAR_MASK;

            debug_assert_eq!(self.is_entry_data_inline::<K>(), false);
            debug_assert_eq!(self.inline_entry_data_len::<K>(), Size(0));
        }
        self.metadata.write_at(memory, self.addr);
        debug_assert_eq!(self.entry_data::<K>(memory), bytes);
    }

    // Don't use this directly, just a helper function for clear() and set_entry_data_raw()
    fn delete_entry_data<K: EntryDataKind>(&mut self, memory: &mut Memory<S>) {
        let data_addr = self.addr + K::offset_within_entry::<C>();

        if !self.is_entry_data_inline::<K>() {
            // Follow the indirection
            let data_addr = Address::read_at(memory, data_addr);
            let len = Size(memory.get_bytes(data_addr, Size(1))[0] as u32);

            let allocation = Allocation::new(data_addr, len + Size(1));

            memory.free(allocation);
        }
    }
}


impl<'m, S: Storage + 'm, C: HashTableConfig> HashTable<'m, S, C> {

    const ENTRY_SIZE: Size = Size(C::MAX_INLINE_KEY_LEN.0 + C::MAX_INLINE_VALUE_LEN.0 + ENTRY_META_SIZE.0);

    #[inline]
    pub fn new(memory: &'m mut Memory<S>) -> HashTable<'m, S, C> {
        HashTable::with_capacity(memory, Size(0))
    }

    pub fn with_capacity(memory: &'m mut Memory<S>, capacity: Size) -> HashTable<'m, S, C> {
        let data = Self::alloc_with_capacity(memory, capacity);

        HashTable {
            data,
            memory,
            config: ::std::marker::PhantomData,
        }
    }

    fn alloc_with_capacity(memory: &'m mut Memory<S>, capacity: Size) -> Allocation {
        let byte_count = Self::byte_count_for_capacity(capacity);
        let data = memory.alloc(byte_count);

        // Write the magic header
        {
            memory.get_bytes_mut(data.addr, Size(4)).copy_from_slice(&MAGIC_HEADER);
        }

        Self::set_len_raw(memory, data, Size(0));
        Self::set_capacity_raw(memory, data, capacity);
        assert!((byte_count - HEADER_SIZE).as_u32() % Self::ENTRY_SIZE.as_u32() == 0);

        data
    }


    #[inline]
    pub fn len(&self) -> usize {
        Self::len_raw(self.memory, self.data).as_usize()
    }

    #[inline]
    pub fn capacity(&self) -> usize {
        Self::capacity_raw(self.memory, self.data).as_usize()
    }

    pub fn find(&self, key: &[u8]) -> Option<&[u8]> {
        let table_size = Self::entry_array_len_raw(self.memory, self.data);
        let hash = hash_for(key);
        let mut entry_index = index_in_table(hash, table_size);

        loop {
            let entry = Self::get_entry_raw(self.memory, self.data, entry_index);

            if entry.is_empty() {
                return None
            } else if entry.hash_equal(hash) &&
                      entry.entry_data::<DataKindKey>(self.memory) == key {
                return Some(entry.entry_data::<DataKindValue>(self.memory))
            }

            entry_index = advance_index(entry_index, table_size);
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

        let table_size = Self::entry_array_len_raw(self.memory, self.data);
        let hash = hash_for(key);
        let mut entry_index = index_in_table(hash, table_size);
        let mut key_added = false;

        for _ in 0 .. table_size {
            let mut entry = Self::get_entry_raw(self.memory, self.data, entry_index);

            if entry.is_empty() {
                entry.init_non_empty(self.memory, hash);
                entry.set_entry_data::<DataKindKey>(self.memory, key);
                entry.set_entry_data::<DataKindValue>(self.memory, value);

                let old_len = Size::from_usize(self.len());
                Self::set_len_raw(self.memory, self.data, old_len + Size(1));
                debug_assert_eq!(Size::from_usize(self.len()), old_len + Size(1));
                key_added = true;
                break
            }

            if entry.hash_equal(hash) &&
               entry.entry_data::<DataKindKey>(self.memory) == key {
                debug_assert!(!entry.is_empty());
                entry.set_entry_data::<DataKindValue>(self.memory, value);
                break
            }

            entry_index = advance_index(entry_index, table_size);
        }

        #[cfg(debug_assertions)]
        {
            let actual_entry = Self::get_entry_raw(self.memory, self.data, entry_index);
            assert!(actual_entry.hash_equal(hash));
            assert!(!actual_entry.is_empty());
            assert_eq!(actual_entry.entry_data::<DataKindKey>(self.memory), key);
            assert_eq!(actual_entry.entry_data::<DataKindValue>(self.memory), value);
            assert_eq!(self.find(key), Some(value));
            self.sanity_check_entry(entry_index);
        }

        key_added
    }

    pub fn remove(&mut self, key: &[u8]) -> bool {
        if self.len() == 0 {
            return false
        }

        let table_size = Self::entry_array_len_raw(self.memory, self.data);
        let hash = hash_for(key);
        let mut index = index_in_table(hash, table_size);

        loop {
            let mut entry = Self::get_entry_raw(self.memory, self.data, index);

            if entry.is_empty() {
                return false
            } else if entry.hash_equal(hash) &&
                      entry.entry_data::<DataKindKey>(self.memory) == key {
                entry.clear(self.memory);

                self.repair_block_after_deletion(index);

                let old_len = Size::from_usize(self.len());
                Self::set_len_raw(self.memory, self.data, old_len - Size(1));

                return true
            }

            index = advance_index(index, table_size);
        }
    }

    fn repair_block_after_deletion(&mut self, deletion_index: u32) {
        let table_size = Self::entry_array_len_raw(self.memory, self.data);

        let mut search_index = advance_index(deletion_index, table_size);

        loop {
            let search_entry = Self::get_entry_raw(self.memory, self.data, search_index);

            if search_entry.is_empty() {
                // nothing to do
                return
            }

            let min_entry_index = index_in_table(search_entry.hash(), table_size);

            if search_index > min_entry_index {
                if deletion_index >= min_entry_index && deletion_index < search_index {
                    self.move_entry(deletion_index, search_entry);
                    self.repair_block_after_deletion(search_index);
                    return
                }
            } else if search_index < min_entry_index {
                if deletion_index >= min_entry_index || deletion_index < search_index {
                    self.move_entry(deletion_index, search_entry);
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

    // Moves src_entry to target_entry_index, clearing src_entry
    fn move_entry(&mut self, target_entry_index: u32, src_entry: Entry<C, S>) {
        assert!(src_entry.addr < self.data.addr + HEADER_SIZE + Self::ENTRY_SIZE * (self.capacity() * 2));
        let target_entry = Self::get_entry_raw(self.memory, self.data, target_entry_index);
        debug_assert!(target_entry.is_empty());
        debug_assert!(!src_entry.is_empty());
        self.memory.copy_nonoverlapping(src_entry.addr, target_entry.addr, Self::ENTRY_SIZE);
        fill_zero(self.memory.get_bytes_mut(src_entry.addr, Self::ENTRY_SIZE));
    }

    fn resize(&mut self, new_capacity: Size) {
        let new_table_data = Self::alloc_with_capacity(self.memory, new_capacity);
        let new_table_size = Self::entry_array_len_raw(self.memory, new_table_data);
        assert_eq!(new_table_size, new_capacity.as_u32() * 2);
        let len = self.len();

        let mut written = 0;

        'outer: for read_index in 0 .. Self::entry_array_len_raw(self.memory, self.data) {
            let read_entry = Self::get_entry_raw(self.memory, self.data, read_index);

            if read_entry.is_empty() {
                // Empty entry, nothing to copy
                continue
            }

            let mut insertion_index = index_in_table(read_entry.hash(), new_table_size);

            for _ in 0 .. new_table_size {
                let new_entry = Self::get_entry_raw(self.memory, new_table_data, insertion_index);

                if new_entry.is_empty() {
                    self.memory.copy_nonoverlapping(read_entry.addr, new_entry.addr, Self::ENTRY_SIZE);

                    // TODO: do some assertions

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
                Self::entry_array_len_raw(self.memory, self.data),
                new_capacity.0,
                new_table_size);
        }

        debug_assert_eq!(written, len);
        Self::set_len_raw(self.memory, new_table_data, Size::from_usize(len));

        self.memory.free(self.data);
        self.data = new_table_data;
    }

    fn sanity_check_entry(&self, entry_index: u32) {
        let entry = Self::get_entry_raw(self.memory, self.data, entry_index);
        if entry.is_empty() {
            return
        }

        let table_size = Self::entry_array_len_raw(self.memory, self.data);
        let min_entry_index = index_in_table(entry.hash(), table_size);

        let mut i = entry_index;
        while i != min_entry_index {
            assert!(!Self::get_entry_raw(self.memory, self.data, i).is_empty(),
            "table_size = {}, index = {}, min_entry_index={}, i={}",
            table_size,
            entry_index,
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
        for index in 0 .. Self::entry_array_len_raw(self.memory, self.data) {
            self.sanity_check_entry(index);
        }
    }

    pub fn iter<F: FnMut(&[u8], &[u8])>(&self, mut f: F) {
        let table_size = Self::entry_array_len_raw(self.memory, self.data);
        for index in 0 .. table_size {
            let entry = Self::get_entry_raw(self.memory, self.data, index);

            if !entry.is_empty() {
                f(entry.entry_data::<DataKindKey>(self.memory),
                  entry.entry_data::<DataKindValue>(self.memory));
            }
        }
    }

    #[inline]
    fn get_entry_raw(storage: &Memory<S>, table_data: Allocation, entry_index: u32) -> Entry<C, S> {
        let entry_addr = Self::entry_addr_raw(table_data, entry_index);
        Entry {
            metadata: u64::read_at(storage, entry_addr),
            addr: entry_addr,
            config: ::std::marker::PhantomData,
            storage: ::std::marker::PhantomData,
        }
    }

    #[inline]
    fn set_len_raw(storage: &mut Memory<S>, table_data: Allocation, len: Size) {
        len.write_at(storage, table_data.addr + LEN_OFFSET);
    }

    #[inline]
    fn set_capacity_raw(storage: &mut Memory<S>, table_data: Allocation, capacity: Size) {
        capacity.write_at(storage, table_data.addr + CAPACITY_OFFSET);
    }

    #[inline]
    fn len_raw(storage: &Memory<S>, table_data: Allocation) -> Size {
        Size::read_at(storage, table_data.addr + LEN_OFFSET)
    }

    #[inline]
    fn capacity_raw(storage: &Memory<S>, table_data: Allocation) -> Size {
        Size::read_at(storage, table_data.addr + CAPACITY_OFFSET)
    }

    #[inline]
    fn entry_array_len_raw(storage: &Memory<S>, table_data: Allocation) -> u32 {
        let capacity = Self::capacity_raw(storage, table_data);
        Self::entry_array_len_for_capacity(capacity)
    }

    #[inline]
    fn entry_addr_raw(table_data: Allocation, entry_index: u32) -> Address {
        table_data.addr + HEADER_SIZE + Self::ENTRY_SIZE * entry_index
    }

    #[inline]
    fn byte_count_for_capacity(capacity: Size) -> Size {
        HEADER_SIZE + Self::ENTRY_SIZE * Self::entry_array_len_for_capacity(capacity)
    }

    #[inline]
    fn entry_array_len_for_capacity(capacity: Size) -> u32 {
        capacity.as_u32() * 2u32
    }
}


#[inline]
fn hash_for(key: &[u8]) -> u64 {
    use metrohash::MetroHash;
    use std::hash::Hasher;
    let mut hasher = MetroHash::default();
    hasher.write(key);
    hasher.finish() as u64
}

#[inline]
fn index_in_table(hash: u64, table_size: u32) -> u32 {
    hash as u32 % table_size
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
        let hash_table: HashTable<_, DefaultHashTableConfig> = HashTable::new(&mut memory);
        assert_eq!(hash_table.len(), 0);
        assert_eq!(hash_table.capacity(), 0);

        hash_table.sanity_check_table();
    }

    #[test]
    fn test_with_capacity() {
        let mut memory = create_memory(10000);
        let hash_table: HashTable<_, DefaultHashTableConfig> = HashTable::with_capacity(&mut memory, Size(100));
        assert_eq!(hash_table.len(), 0);
        assert_eq!(hash_table.capacity(), 100);

        hash_table.sanity_check_table();
    }
}
