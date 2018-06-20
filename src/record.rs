
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
    pub fn first_free(&self) -> RecordId {
        RecordId::read_at(self.storage, self.data.addr + FIRST_FREE_OFFSET)
    }

    #[inline]
    pub fn get_record(&self, id: RecordId) -> Record {
        assert!(id.0 > 0 && id.0 < self.array_len().as_u32());
        let addr = self.data.addr + ARRAY_OFFSET + RECORD_SIZE * id.idx();
        Record::read_at(self.storage, addr)
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

    pub fn init_at(storage: &'s Memory<S>, addr: Address, records: &[Record]) -> RecordTableMut<'s, S> {

        let item_count = Size::from_usize(records.len());
        let array_len = item_count + Size(1);
        let data_size = ARRAY_OFFSET + array_len * RECORD_SIZE;

        item_count.write_at(storage, addr + ITEM_COUNT_OFFSET);
        array_len.write_at(storage, addr + ARRAY_LEN_OFFSET);
        RecordId(0).write_at(storage, addr + FIRST_FREE_OFFSET);

        let mut table = RecordTableMut {
            storage,
            data: Allocation::new(addr, data_size),
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
        record.write_at(self.storage, addr);
    }

    #[inline]
    pub fn readonly(&'s self) -> RecordTable<'s, S> {
        RecordTable {
            storage: self.storage,
            data: self.data,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_storage(record_count: usize) -> Memory<MemStore> {
        let size = ARRAY_OFFSET + RECORD_SIZE * (record_count + 1) + Size(1);
        Memory::new(MemStore::new(size.as_usize()))
    }

    // #[test]
    // fn test_init_at() {

    //     let records = [
    //         Record {
    //             addr: Address(1010),
    //             size: Size(2323),
    //             ref_count: 3432,
    //         },
    //         Record {
    //             addr: Address(76),
    //             size: Size(34324),
    //             ref_count: 23,
    //         },
    //         Record {
    //             addr: Address(743),
    //             size: Size(23),
    //             ref_count: 8,
    //         },
    //     ];

    //     let mut storage = create_storage(records.len());

    //     let record_table = RecordTableMut::init_at(&mut storage, Address(1), &records[..]);

    //     assert_eq!(record_table.item_count(), Size(3));
    //     assert_eq!(record_table.array_len(),  Size(4));
    //     assert_eq!(record_table.first_free(), RecordId(0));

    //     for i in 0 .. records.len() {
    //         let record_id = RecordId((i + 1) as u32);

    //         assert_eq!(records[i], record_table.get_record(record_id));
    //     }
    // }
}
