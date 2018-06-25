
use std::mem;
use memory::*;
use allocator::*;
use persist::*;

// TODO: non-zero
#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct RecordId(u32);

impl RecordId {
    #[inline(always)]
    pub(crate) fn idx(self) -> usize {
        self.0 as usize
    }

    #[inline(always)]
    pub(crate) fn from_usize(idx: usize) -> RecordId {
        assert!(idx <= ::std::u32::MAX as usize);
        RecordId(idx as u32)
    }
}


impl Serialize for RecordId {
    #[inline]
    fn write<'s, S: Storage + 's>(&self, writer: &mut StorageWriter<'s, S>) {
        writer.write_u32(self.0);
    }
}

impl Deserialize for RecordId {
    #[inline]
    fn read<'s, S: Storage + 's>(reader: &mut StorageReader<'s, S>) -> RecordId {
        RecordId(reader.read_u32())
    }
}

#[derive(Clone, Copy, Eq, PartialEq, Hash, Debug)]
pub(crate) struct Record {
    pub addr: Address,
    pub size: Size,
    pub ref_count: u32,
}

impl Record {
    pub fn null() -> Record {
        Record {
            addr: Address(0),
            size: Size(0),
            ref_count: 0,
        }
    }
}

const EMPTY_RECORD_ADDRESS: Address = Address(0);
const PENDING_RECORD_ADDRESS: Address = Address(0);

impl Serialize for Record {
    #[inline]
    fn write<'s, S: Storage + 's>(&self, writer: &mut StorageWriter<'s, S>) {
        let Record {
            addr,
            size,
            ref_count,
        } = *self;

        addr.write(writer);
        size.write(writer);
        ref_count.write(writer);
    }
}

impl Deserialize for Record {
    #[inline]
    fn read<'s, S: Storage + 's>(reader: &mut StorageReader<'s, S>) -> Record {
        let addr = Address::read(reader);
        let size = Size::read(reader);
        let ref_count = u32::read(reader);

        Record {
            addr,
            size,
            ref_count,
        }
    }
}


pub(crate) struct RecordTable<'s, S: Storage + 's> {
    storage: &'s Memory<S>,
    data: Allocation,
}

pub(crate) struct RecordTableMut<'s, S: Storage + 's> {
    storage: &'s Memory<S>,
    data: Allocation,
}

const ITEM_COUNT_OFFSET: Size = Size(0);
const ARRAY_LEN_OFFSET: Size = Size(4);
const FIRST_FREE_OFFSET: Size = Size(8);
const ARRAY_OFFSET: Size = Size(12);
const RECORD_SIZE: Size = Size(mem::size_of::<Record>() as u32);

const FREE_PTR_OFFSET_WITHIN_RECORD: Size = Size(4);

impl<'s, S: Storage + 's> RecordTable<'s, S> {

    #[inline]
    pub fn at(storage: &'s Memory<S>, addr: Address, size: Size) -> RecordTable<'s, S> {
        RecordTable {
            storage,
            data: Allocation::new(addr, size),
        }
    }

    #[inline]
    pub fn item_count(&self) -> Size {
        Size::read_at(self.storage, self.data.addr + ITEM_COUNT_OFFSET)
    }

    #[inline]
    pub fn array_len(&self) -> Size {
        Size::read_at(self.storage, self.data.addr + ARRAY_LEN_OFFSET)
    }

    #[inline]
    pub fn get_record(&self, id: RecordId) -> Record {
        assert!(id.0 > 0 && id.0 < self.array_len().as_u32());
        let addr = self.data.addr + ARRAY_OFFSET + RECORD_SIZE * id.idx();
        let record = Record::read_at(self.storage, addr);
        assert!(record.addr != EMPTY_RECORD_ADDRESS);
        assert!(record.addr != PENDING_RECORD_ADDRESS);
        record
    }
}


impl<'s, S: Storage + 's> RecordTableMut<'s, S> {

    #[inline]
    pub fn at(storage: &'s Memory<S>, addr: Address, size: Size) -> RecordTableMut<'s, S> {
        RecordTableMut {
            storage,
            data: Allocation::new(addr, size),
        }
    }

    pub fn alloc(storage: &'s Memory<S>, records: &[Record]) -> RecordTableMut<'s, S> {

        let item_count = Size::from_usize(records.len());
        let array_len = item_count + Size(1);
        let table_byte_size = ARRAY_OFFSET + array_len * RECORD_SIZE;

        let alloc = storage.alloc(table_byte_size);

        item_count.write_at(storage, alloc.addr + ITEM_COUNT_OFFSET);
        array_len.write_at(storage, alloc.addr + ARRAY_LEN_OFFSET);
        RecordId(0).write_at(storage, alloc.addr + FIRST_FREE_OFFSET);

        let mut table = RecordTableMut {
            storage,
            data: alloc,
        };

        for (index, &record) in records.iter().enumerate() {
            let record_id = RecordId(index as u32 + 1);
            table.set_record(record_id, record);
        }

        table
    }

    #[inline]
    pub fn item_count(&self) -> Size {
        self.readonly().item_count()
    }

    #[inline]
    pub fn array_len(&self) -> Size {
        self.readonly().array_len()
    }

    #[inline]
    pub fn first_free(&self) -> RecordId {
        RecordId::read_at(self.storage, self.data.addr + FIRST_FREE_OFFSET)
    }

    #[inline]
    pub fn get_record(&self, id: RecordId) -> Record {
        self.readonly().get_record(id)
    }

    #[inline]
    pub fn set_record(&mut self, id: RecordId, record: Record) {
        assert!(id.0 > 0 && id.0 < self.array_len().as_u32());
        let addr = self.data.addr + ARRAY_OFFSET + RECORD_SIZE * id.idx();
        assert_ne!(Address::read_at(self.storage, addr), EMPTY_RECORD_ADDRESS);
        record.write_at(self.storage, addr);
    }

    #[inline]
    pub fn alloc_record(&mut self) -> RecordId {
        // Expand size if necessary
        if self.first_free() == RecordId(0) {
            let item_count = self.item_count();
            let old_array_len = self.array_len();
            debug_assert_eq!(old_array_len, item_count + Size(1));
            let new_max_item_count = if item_count == Size(0) {
                Size(8)
            } else {
                item_count * 2u32
            };
            let new_alloc = self.storage.alloc(record_table_alloc_size_for(new_max_item_count.as_usize()));
            self.storage.copy_nonoverlapping(self.data.addr, new_alloc.addr, self.data.size);
            fill_zero(&mut self.storage.get_bytes_mut(new_alloc.addr + self.data.size, new_alloc.size - self.data.size));
            let new_array_len = new_max_item_count + Size(1u32);
            new_array_len.write_at(self.storage, new_alloc.addr + ARRAY_LEN_OFFSET);

            let mut free_ptr = new_alloc.addr + FIRST_FREE_OFFSET;
            println!("&first_free = {:?}, array_len_before={:?}", free_ptr, old_array_len);
            for free_record in old_array_len.as_u32() .. new_array_len.as_u32() {
                let record_id = RecordId(free_record);
                record_id.write_at(self.storage, free_ptr);
                free_ptr = new_alloc.addr + ARRAY_OFFSET + RECORD_SIZE * free_record + FREE_PTR_OFFSET_WITHIN_RECORD;
                println!("record_id = {:?}, free_ptr = {:?}", record_id, free_ptr);
            }

            self.storage.free(self.data);
            self.data = new_alloc;

            #[cfg(debug_assertions)]
            {
                assert_eq!(self.item_count(), item_count);

                let all_free = self.all_free();
                let expected: Vec<_> = (old_array_len.as_u32() .. new_array_len.as_u32())
                    .map(|i| RecordId(i))
                    .collect();
                assert_eq!(all_free, expected);
            }
        }

        let new_id = {
            assert!(self.first_free() != RecordId(0));

            let free_id = self.first_free();

            let record_addr = self.record_addr(free_id);
            assert!(Address::read_at(self.storage, record_addr) == Address(0));
            let next_free = RecordId::read_at(self.storage, record_addr + FREE_PTR_OFFSET_WITHIN_RECORD);
            next_free.write_at(self.storage, self.data.addr + FIRST_FREE_OFFSET);

            free_id
        };

        PENDING_RECORD_ADDRESS.write_at(self.storage, self.record_addr(new_id));

        (self.item_count() + Size(1)).write_at(self.storage, self.data.addr + ITEM_COUNT_OFFSET);

        new_id
    }

    pub fn delete_record(&mut self, record_id: RecordId) -> Record {
        #[cfg(debug_assertions)]
        {
            self.iter_free(|id| assert!(record_id != id));
        }

        let record_addr = self.record_addr(record_id);
        let deleted_record = Record::read_at(self.storage, record_addr);
        assert!(deleted_record.addr != EMPTY_RECORD_ADDRESS);
        assert!(deleted_record.addr != PENDING_RECORD_ADDRESS);

        fill_zero(&mut self.storage.get_bytes_mut(record_addr, RECORD_SIZE));

        self.first_free().write_at(self.storage, record_addr + FREE_PTR_OFFSET_WITHIN_RECORD);
        record_id.write_at(self.storage, self.data.addr + FIRST_FREE_OFFSET);
        (self.item_count() - Size(1)).write_at(self.storage, self.data.addr + ITEM_COUNT_OFFSET);

        assert!(Address::read_at(self.storage, record_addr) == Address(0));

        #[cfg(debug_assertions)]
        {
            let mut found = false;
            self.iter_free(|id| found = found || (id == record_id));
            assert!(found);
        }

        deleted_record
    }

    #[inline]
    pub fn readonly(&'s self) -> RecordTable<'s, S> {
        RecordTable {
            storage: self.storage,
            data: self.data,
        }
    }

    #[inline]
    fn record_addr(&self, id: RecordId) -> Address {
        assert!(id.0 > 0 && id.0 < self.array_len().as_u32(),
            "id={:?}, array_len={:?}", id, self.array_len());
        self.data.addr + ARRAY_OFFSET + RECORD_SIZE * id.idx()
    }

    fn all_free(&self) -> Vec<RecordId> {
        let mut result = vec![];

        self.iter_free(|id| result.push(id));

        result.sort();

        result
    }

    fn iter_free<F: FnMut(RecordId)>(&self, mut f: F) {
        let mut free_ptr = self.first_free();

        while free_ptr != RecordId(0) {
            f(free_ptr);
            free_ptr = RecordId::read_at(self.storage, self.record_addr(free_ptr ) + FREE_PTR_OFFSET_WITHIN_RECORD);
        }
    }
}

fn record_table_alloc_size_for(record_count: usize) -> Size {
    ARRAY_OFFSET + RECORD_SIZE * (record_count + 1)
}


pub(crate) fn persist_record_table<S: Storage>(memory: &Memory<S>,
                                               records: Vec<Record>,
                                               record_id_free_list: Vec<RecordId>)
                                               -> Address {
//     let alloc = memory.alloc(record_table_alloc_size_for(records.len()));

//     let mut writer = StorageWriter::new(storage, alloc.addr);

//     Size::from_usize(records.len() - record_id_free_list.len()).write(&mut writer);
//     Size::from_usize(records.len()).write(&mut writer);
//     Size::from_usize(records.len()).write(&mut writer);

// //     const ITEM_COUNT_OFFSET: Size = Size(0);
// // const ARRAY_LEN_OFFSET: Size = Size(4);
// // const FIRST_FREE_OFFSET: Size = Size(8);
// // const ARRAY_OFFSET: Size = Size(12);

    panic!()
}

pub(crate) struct RuntimeRecordTable<S: Storage> {
    data: Allocation,
    storage: ::std::marker::PhantomData<S>,
}

impl<S: Storage> RuntimeRecordTable<S> {
    pub(crate) fn with<R, F: FnOnce(&RecordTable<S>) -> R>(&self, memory: &Memory<S>, f: F) -> R {
        let record_table = RecordTable::at(memory, self.data.addr, self.data.size);
        f(&record_table)
    }

    pub(crate) fn with_mut<R, F: FnOnce(&mut RecordTableMut<S>) -> R>(&self, memory: &Memory<S>, f: F) -> R {
        assert!(!S::IS_READONLY);
        let record_table = RecordTableMut::at(memory, self.data.addr, self.data.size);
        let result = f(&mut record_table);
        self.data = record_table.data;
        result
    }

    pub(crate) fn from(table: RecordTableMut<S>) -> RuntimeRecordTable<S> {
        RuntimeRecordTable {
            data: table.data,
            storage: ::std::marker::PhantomData,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_storage(record_count: usize) -> Memory<MemStore> {
        let size = ARRAY_OFFSET + RECORD_SIZE * (record_count + 1) + Size(1);
        let memory = Memory::new(MemStore::new(size.as_usize()));
        memory.alloc(Size(1));
        memory
    }

    #[test]
    fn test_alloc_table() {

        let records = [
            Record {
                addr: Address(1010),
                size: Size(2323),
                ref_count: 3432,
            },
            Record {
                addr: Address(76),
                size: Size(34324),
                ref_count: 23,
            },
            Record {
                addr: Address(743),
                size: Size(23),
                ref_count: 8,
            },
        ];

        let storage = create_storage(records.len());

        let record_table = RecordTableMut::alloc(&storage, &records[..]);

        assert_eq!(record_table.item_count(), Size(3));
        assert_eq!(record_table.array_len(),  Size(4));
        assert_eq!(record_table.first_free(), RecordId(0));

        for i in 0 .. records.len() {
            let record_id = RecordId((i + 1) as u32);

            assert_eq!(records[i], record_table.get_record(record_id));
        }
    }

    #[test]
    fn test_alloc_record() {

        let storage = create_storage(500);

        let mut record_table = RecordTableMut::alloc(&storage, &[]);

        assert_eq!(record_table.item_count(), Size(0));
        assert_eq!(record_table.array_len(),  Size(1));
        assert_eq!(record_table.first_free(), RecordId(0));

        let mut records = vec![];

        for i in 0 .. 100 {
            let record = Record {
                addr: Address(i * 7 + 1),
                size: Size(i * 3),
                ref_count: i * 11,
            };

            let id = record_table.alloc_record();
            record_table.set_record(id, record);

            records.push((id, record));
        }

        for (id, record) in records {
            assert_eq!(record_table.get_record(id), record);
        }
    }

    #[test]
    fn test_delete_record() {

        let storage = create_storage(300);

        let mut record_table = RecordTableMut::alloc(&storage, &[]);

        let mut records = vec![];

        for i in 0 .. 100 {
            let record = Record {
                addr: Address(i * 7 + 1),
                size: Size(i * 3),
                ref_count: i * 11,
            };

            let id = record_table.alloc_record();
            record_table.set_record(id, record);

            records.push((id, record));
        }

        let mut free_records = record_table.all_free();

        for (id, record) in records {
            let deleted_record = record_table.delete_record(id);

            assert_eq!(record, deleted_record);

            free_records.push(id);
            free_records.sort();

            assert_eq!(free_records, record_table.all_free());
        }
    }
}
