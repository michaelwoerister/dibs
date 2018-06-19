
use std::mem;
use std::marker::PhantomData;
use byteorder::{LittleEndian, ByteOrder};
use allocator::Allocation;
use persist::*;
use memory::*;

pub struct HashTable<'m, S: Storage + 'm, C: HashTableConfig = DefaultHashTableConfig> {
    data: Allocation,
    memory: &'m mut Memory<S>,
    config: PhantomData<C>,
}

impl<'m, S: Storage + 'm, C: HashTableConfig> HashTable<'m, S, C> {

    #[inline]
    pub fn new(memory: &'m mut Memory<S>) -> HashTable<'m, S, C> {
        HashTable::with_capacity(memory, Size(0))
    }

    #[inline]
    pub fn with_capacity(memory: &'m mut Memory<S>, capacity: Size) -> HashTable<'m, S, C> {
        let data = RawTable::<S, C>::alloc_with_capacity(memory, capacity);

        HashTable {
            data,
            memory,
            config: PhantomData,
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        RawTable::<S, C>::len(self.memory, self.data).as_usize()
    }

    #[inline]
    pub fn capacity(&self) -> usize {
        RawTable::<S, C>::capacity(self.memory, self.data).as_usize()
    }

    pub fn find(&self, key: &[u8]) -> Option<&[u8]> {
        RawTable::<S, C>::find(self.memory, self.data, key)
    }

    pub fn insert(&mut self, key: &[u8], value: &[u8]) -> bool {
        RawTable::<S, C>::insert(self.memory, &mut self.data, key, value)
    }

    #[inline]
    pub fn remove(&mut self, key: &[u8]) -> bool {
        RawTable::<S, C>::remove_entry(self.memory, self.data, key)
    }

    #[inline]
    pub fn delete_table(self) {
        RawTable::<S, C>::delete_table(self.memory, self.data);
    }

    pub fn sanity_check_table(&self) {
        RawTable::<S, C>::sanity_check_table(self.memory, self.data);
    }

    pub fn iter<F: FnMut(&[u8], &[u8])>(&self, f: F) {
        RawTable::<S, C>::iter(self.memory, self.data, f);
    }
}



const MAGIC_HEADER: [u8; 4] = [b'H', b'A', b'S', b'H'];

const MAGIC_HEADER_OFFSET: Size = Size(0);
const LEN_OFFSET: Size = Size(MAGIC_HEADER_OFFSET.0 + 4);
const CAPACITY_OFFSET: Size = Size(LEN_OFFSET.0 + 4);

const HEADER_SIZE: Size = Size(CAPACITY_OFFSET.0 + 4);
const ENTRY_META_SIZE: Size = Size(8);

// Layout:
//
// magic_header: u32
// item_count: u32
// capacity: u32
// entry*
pub struct RawTable<S: Storage, C: HashTableConfig = DefaultHashTableConfig> {
    memory: PhantomData<S>,
    config: PhantomData<C>,
}

pub trait HashTableConfig {
    const MAX_INLINE_KEY_LEN: Size = Size(4);
    const MAX_INLINE_VALUE_LEN: Size = Size(4);
    const ENTRY_SIZE: Size = Size(Self::MAX_INLINE_KEY_LEN.0 +
                                  Self::MAX_INLINE_VALUE_LEN.0 +
                                  ENTRY_META_SIZE.0);
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
    config: PhantomData<C>,
    storage: PhantomData<S>,
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
        fill_zero(memory.get_bytes_mut(self.addr, C::ENTRY_SIZE));
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

    // Don't use this directly, just a helper function for clear() and set_entry_data()
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

impl<S: Storage, C: HashTableConfig> RawTable<S, C> {

    fn alloc_with_capacity(memory: &mut Memory<S>, capacity: Size) -> Allocation {
        let byte_count = Self::byte_count_for_capacity(capacity);
        let data = memory.alloc(byte_count);

        // Write the magic header
        {
            memory.get_bytes_mut(data.addr, Size(4)).copy_from_slice(&MAGIC_HEADER);
        }

        Self::set_len(memory, data, Size(0));
        Self::set_capacity(memory, data, capacity);
        assert!((byte_count - HEADER_SIZE).as_u32() % C::ENTRY_SIZE.as_u32() == 0);

        data
    }

    fn find<'m>(memory: &'m Memory<S>, table_data: Allocation, key: &[u8]) -> Option<&'m [u8]> {
        let table_size = Self::entry_array_len(memory, table_data);
        let hash = hash_for(key);
        let mut entry_index = index_in_table(hash, table_size);

        loop {
            let entry = Self::get_entry(memory, table_data, entry_index);

            if entry.is_empty() {
                return None
            } else if entry.hash_equal(hash) &&
                      entry.entry_data::<DataKindKey>(memory) == key {
                return Some(entry.entry_data::<DataKindValue>(memory))
            }

            entry_index = advance_index(entry_index, table_size);
        }
    }

    pub fn insert(memory: &mut Memory<S>, table_data: &mut Allocation, key: &[u8], value: &[u8]) -> bool {
        let initial_capacity = Self::capacity(memory, *table_data);
        if Self::len(memory, *table_data) >= initial_capacity {
            let new_capacity = if initial_capacity == Size(0) {
                Size(8)
            } else {
                (initial_capacity * 3u32) / 2u32
            };
            debug_assert!(new_capacity > Size(0));
            Self::resize(memory, table_data, new_capacity);
        }

        let table_size = Self::entry_array_len(memory, *table_data);
        let hash = hash_for(key);
        let mut entry_index = index_in_table(hash, table_size);
        let mut key_added = false;

        for _ in 0 .. table_size {
            let mut entry = Self::get_entry(memory, *table_data, entry_index);

            if entry.is_empty() {
                entry.init_non_empty(memory, hash);
                entry.set_entry_data::<DataKindKey>(memory, key);
                entry.set_entry_data::<DataKindValue>(memory, value);

                let old_len = Self::len(memory, *table_data);
                Self::set_len(memory, *table_data, old_len + Size(1));
                debug_assert_eq!(Self::len(memory, *table_data), old_len + Size(1));
                key_added = true;
                break
            }

            if entry.hash_equal(hash) &&
               entry.entry_data::<DataKindKey>(memory) == key {
                debug_assert!(!entry.is_empty());
                entry.set_entry_data::<DataKindValue>(memory, value);
                break
            }

            entry_index = advance_index(entry_index, table_size);
        }

        #[cfg(debug_assertions)]
        {
            let actual_entry = Self::get_entry(memory, *table_data, entry_index);
            assert!(actual_entry.hash_equal(hash));
            assert!(!actual_entry.is_empty());
            assert_eq!(actual_entry.entry_data::<DataKindKey>(memory), key);
            assert_eq!(actual_entry.entry_data::<DataKindValue>(memory), value);
            assert_eq!(Self::find(memory, *table_data, key), Some(value));
            Self::sanity_check_entry(memory, *table_data, entry_index);
        }

        key_added
    }

    fn delete_table(memory: &mut Memory<S>, table_data: Allocation) {
        let table_size = Self::entry_array_len(memory, table_data);

        for entry_index in 0 .. table_size {
            let mut entry = Self::get_entry(memory, table_data, entry_index);
            if !entry.is_empty() {
                entry.clear(memory);
            }
        }

        memory.free(table_data);
    }

    fn remove_entry(memory: &mut Memory<S>, table_data: Allocation, key: &[u8]) -> bool {
        if Self::len(memory, table_data) == Size(0) {
            return false
        }

        let table_size = Self::entry_array_len(memory, table_data);
        let hash = hash_for(key);
        let mut index = index_in_table(hash, table_size);

        loop {
            let mut entry = Self::get_entry(memory, table_data, index);

            if entry.is_empty() {
                return false
            } else if entry.hash_equal(hash) &&
                      entry.entry_data::<DataKindKey>(memory) == key {
                entry.clear(memory);

                Self::repair_block_after_deletion(memory, table_data, index);

                let old_len = Self::len(memory, table_data);
                Self::set_len(memory, table_data, old_len - Size(1));

                return true
            }

            index = advance_index(index, table_size);
        }
    }

    fn repair_block_after_deletion(memory: &mut Memory<S>, table_data: Allocation, deletion_index: u32) {
        let table_size = Self::entry_array_len(memory, table_data);

        let mut search_index = advance_index(deletion_index, table_size);

        loop {
            let search_entry = Self::get_entry(memory, table_data, search_index);

            if search_entry.is_empty() {
                // nothing to do
                return
            }

            let min_entry_index = index_in_table(search_entry.hash(), table_size);

            if search_index > min_entry_index {
                if deletion_index >= min_entry_index && deletion_index < search_index {
                    Self::move_entry(memory, table_data, deletion_index, search_entry);
                    Self::repair_block_after_deletion(memory, table_data, search_index);
                    return
                }
            } else if search_index < min_entry_index {
                if deletion_index >= min_entry_index || deletion_index < search_index {
                    Self::move_entry(memory, table_data, deletion_index, search_entry);
                    Self::repair_block_after_deletion(memory, table_data, search_index);
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
    fn move_entry(memory: &mut Memory<S>, table_data: Allocation, target_entry_index: u32, src_entry: Entry<C, S>) {
        Self::assert_is_valid_entry_for_table(memory, table_data, &src_entry);
        let target_entry = Self::get_entry(memory, table_data, target_entry_index);
        debug_assert!(target_entry.is_empty());
        debug_assert!(!src_entry.is_empty());
        memory.copy_nonoverlapping(src_entry.addr, target_entry.addr, C::ENTRY_SIZE);
        fill_zero(memory.get_bytes_mut(src_entry.addr, C::ENTRY_SIZE));
    }

    fn assert_is_valid_entry_for_table(memory: &Memory<S>, table_data: Allocation, entry: &Entry<C, S>) {
        let entry_array_start = table_data.addr + HEADER_SIZE;
        let last_valid_entry_addr = entry_array_start + C::ENTRY_SIZE * (Self::entry_array_len(memory, table_data) - 1);
        debug_assert!(entry.addr >= entry_array_start && entry.addr <= last_valid_entry_addr);
        debug_assert!((entry.addr.as_u32() - entry_array_start.as_u32()) % C::ENTRY_SIZE.as_u32() == 0,
            "misaligned entry addr");
    }

    fn resize(memory: &mut Memory<S>, table_data: &mut Allocation, new_capacity: Size) {
        let new_table_data = Self::alloc_with_capacity(memory, new_capacity);
        let new_table_size = Self::entry_array_len(memory, new_table_data);
        debug_assert!(new_table_size > 0);
        assert_eq!(new_table_size, Self::entry_array_len_for_capacity(new_capacity));
        let len = Self::len(memory, *table_data);

        let mut written = 0;

        'outer: for read_index in 0 .. Self::entry_array_len(memory, *table_data) {
            let read_entry = Self::get_entry(memory, *table_data, read_index);

            if read_entry.is_empty() {
                // Empty entry, nothing to copy
                continue
            }

            let mut insertion_index = index_in_table(read_entry.hash(), new_table_size);

            for _ in 0 .. new_table_size {
                let new_entry = Self::get_entry(memory, new_table_data, insertion_index);

                if new_entry.is_empty() {
                    memory.copy_nonoverlapping(read_entry.addr, new_entry.addr, C::ENTRY_SIZE);

                    // TODO: do some assertions

                    written += 1;
                    debug_assert!(written <= len.as_usize(),
                        "more non-null entries than len() in table. \
                         written = {}, len={}", written, len.as_usize());
                    continue 'outer
                }

                insertion_index = advance_index(insertion_index, new_table_size);
            }

            panic!("no free entry found? len={}, old_capacity={}, \
                    old_table_size={}, new_capacity={}, new_table_size={}",
                len.as_usize(),
                Self::capacity(memory, *table_data).as_usize(),
                Self::entry_array_len(memory, *table_data),
                new_capacity.0,
                new_table_size);
        }

        debug_assert_eq!(written, len.as_usize());
        Self::set_len(memory, new_table_data, len);

        memory.free(*table_data);
        *table_data = new_table_data;
    }

    fn sanity_check_entry(memory: &Memory<S>, table_data: Allocation, entry_index: u32) {
        let entry = Self::get_entry(memory, table_data, entry_index);
        if entry.is_empty() {
            return
        }

        let table_size = Self::entry_array_len(memory, table_data);
        let min_entry_index = index_in_table(entry.hash(), table_size);

        let mut i = entry_index;
        while i != min_entry_index {
            assert!(!Self::get_entry(memory, table_data, i).is_empty(),
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

    fn sanity_check_table(memory: &Memory<S>, table_data: Allocation) {
        for index in 0 .. Self::entry_array_len(memory, table_data) {
            Self::sanity_check_entry(memory, table_data, index);
        }
    }

    fn iter<F: FnMut(&[u8], &[u8])>(memory: &Memory<S>, table_data: Allocation, mut f: F) {
        let table_size = Self::entry_array_len(memory, table_data);
        for index in 0 .. table_size {
            let entry = Self::get_entry(memory, table_data, index);

            if !entry.is_empty() {
                f(entry.entry_data::<DataKindKey>(memory),
                  entry.entry_data::<DataKindValue>(memory));
            }
        }
    }

    #[inline]
    fn get_entry(memory: &Memory<S>, table_data: Allocation, entry_index: u32) -> Entry<C, S> {
        debug_assert!(entry_index < Self::entry_array_len(memory, table_data));
        let entry_addr = Self::entry_addr(table_data, entry_index);
        Entry {
            metadata: u64::read_at(memory, entry_addr),
            addr: entry_addr,
            config: PhantomData,
            storage: PhantomData,
        }
    }

    #[inline]
    fn set_len(storage: &mut Memory<S>, table_data: Allocation, len: Size) {
        len.write_at(storage, table_data.addr + LEN_OFFSET);
    }

    #[inline]
    fn set_capacity(storage: &mut Memory<S>, table_data: Allocation, capacity: Size) {
        capacity.write_at(storage, table_data.addr + CAPACITY_OFFSET);
    }

    #[inline]
    fn len(storage: &Memory<S>, table_data: Allocation) -> Size {
        Size::read_at(storage, table_data.addr + LEN_OFFSET)
    }

    #[inline]
    fn capacity(storage: &Memory<S>, table_data: Allocation) -> Size {
        Size::read_at(storage, table_data.addr + CAPACITY_OFFSET)
    }

    #[inline]
    fn entry_array_len(storage: &Memory<S>, table_data: Allocation) -> u32 {
        let capacity = Self::capacity(storage, table_data);
        Self::entry_array_len_for_capacity(capacity)
    }

    #[inline]
    fn entry_addr(table_data: Allocation, entry_index: u32) -> Address {
        table_data.addr + HEADER_SIZE + C::ENTRY_SIZE * entry_index
    }

    #[inline]
    fn byte_count_for_capacity(capacity: Size) -> Size {
        HEADER_SIZE + C::ENTRY_SIZE * Self::entry_array_len_for_capacity(capacity)
    }

    #[inline]
    fn entry_array_len_for_capacity(capacity: Size) -> u32 {
        (capacity.as_u32() * 3) / 2
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
