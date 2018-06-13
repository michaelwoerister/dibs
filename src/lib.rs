
extern crate byteorder;
extern crate metrohash;

#[cfg(test)]
extern crate rand;

use std::collections::HashSet;

mod buffer;
mod address;
mod allocator;
mod hashtable;

pub use address::{Address, Size};
pub use buffer::{Buffer, BufferProvider};
pub use allocator::{Allocator, Allocation};
pub use hashtable::HashTable;

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

pub struct Encoder<'buf, 'db, S: Storage + 'db> {
    db: &'db mut Database<S>,
    buffer: Buffer<'buf>,
    referenced_records: HashSet<RecordId>,
}

impl<'buf, 'db, S: Storage + 'db> Encoder<'buf, 'db, S> {

    #[inline(always)]
    pub fn buffer(&mut self) -> &mut Buffer<'buf> {
        &mut self.buffer
    }

    pub fn write_record<W>(&mut self, write: W) -> RecordId
        where W: FnOnce(&mut Encoder<'_, '_, S>, &mut CurrentRecordId)
    {
        let record_id = self.db.alloc_record();

        let mut encoder = Encoder {
            db: self.db,
            buffer: self.buffer.start_sub_buffer(),
            referenced_records: HashSet::new(),
        };

        let mut current_record_id = CurrentRecordId {
            record_id,
            was_accessed: false,
        };

        write(&mut encoder, &mut current_record_id);

        let Encoder {
            db,
            buffer,
            // TODO: implement GC
            referenced_records: _
        } = encoder;

        let record_size = buffer.len();
        let allocation = db.alloc.alloc(record_size);

        db.storage.write_bytes(allocation.addr, buffer.bytes());

        {
            let record = &mut db.records[record_id.idx()];
            record.addr = allocation.addr;
            record.size = allocation.size;
        }

        record_id
    }

    #[inline]
    pub fn write_record_id(&mut self, id: RecordId) {
        self.db.records[id.idx()].ref_count += 1;
        self.referenced_records.insert(id);
    }
}

// TODO: non-zero
#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct RecordId(u32);

impl RecordId {
    #[inline(always)]
    fn idx(self) -> usize {
        self.0 as usize
    }

    #[inline(always)]
    fn from_usize(idx: usize) -> RecordId {
        assert!(idx <= ::std::u32::MAX as usize);
        RecordId(idx as u32)
    }
}

#[derive(Clone, Copy, Eq, PartialEq, Hash, Debug)]
struct Record {
    addr: Address,
    size: Size,
    ref_count: u32,
}

impl Record {
    fn null() -> Record {
        Record {
            addr: Address(0),
            size: Size(0),
            ref_count: 0,
        }
    }
}

pub struct CurrentRecordId {
    record_id: RecordId,
    was_accessed: bool,
}


impl CurrentRecordId {

    pub fn get(&mut self) -> RecordId {
        self.was_accessed = true;
        self.record_id
    }
}

pub struct Database<S: Storage> {
    storage: S,
    record_id_free_list: Vec<RecordId>,
    records: Vec<Record>,
    alloc: Allocator,
    buffer_providers: Vec<BufferProvider>,
}

impl<S: Storage> Database<S> {

    pub fn new(storage: S) -> Database<S> {
        Database {
            storage,
            record_id_free_list: Vec::new(),
            records: Vec::new(),
            alloc: Allocator::new(Size(0)),
            buffer_providers: Vec::new(),
        }
    }

    fn alloc_record(&mut self) -> RecordId {
        if let Some(record_id) = self.record_id_free_list.pop() {
            assert_eq!(self.records[record_id.idx()], Record::null());
            record_id
        } else {
            self.records.push(Record::null());
            RecordId::from_usize(self.records.len())
        }
    }

    pub fn get_record(&self, record_id: RecordId) -> &[u8] {
        let record = &self.records[record_id.idx()];
        self.storage.get_bytes(record.addr, record.size)
    }

    pub fn write_record<W>(&mut self, w: W) -> RecordId
        where W: FnOnce(&mut Encoder<'_, '_, S>, &mut CurrentRecordId)
    {
        let mut buffer_provider = self.buffer_providers
                                      .pop()
                                      .unwrap_or_else(|| BufferProvider::new());
        let record_id = {
            let mut encoder = Encoder {
                db: self,
                buffer: buffer_provider.get_buffer(),
                referenced_records: HashSet::new(),
            };

            encoder.write_record(w)
        };

        self.buffer_providers.push(buffer_provider);

        record_id
    }

    pub fn delete_record(&mut self, record_id: RecordId) {
        let record = self.records[record_id.idx()];
        self.alloc.free(Allocation::new(record.addr, record.size));
        self.records[record_id.idx()] = Record::null();
        self.record_id_free_list.push(record_id);
    }
}

impl<S: Storage> Drop for Database<S> {
    fn drop(&mut self) {

    }
}





