
extern crate byteorder;
extern crate metrohash;
extern crate parking_lot;

#[macro_use]
extern crate bitflags;

#[cfg(test)]
extern crate rand;

use std::collections::HashSet;
use std::mem;

mod allocator;
mod buffer;
mod footer;
mod hashtable;
mod header;
mod memory;
mod persist;
mod record;

pub use allocator::{Allocator, Allocation};
pub use buffer::{Buffer, BufferProvider};
pub use hashtable::{HashTable, HashTableConfig, DefaultHashTableConfig};
pub use memory::*;
use record::{Record, RecordId};

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
        let allocation = db.memory.alloc(record_size);

        db.memory.get_bytes_mut(allocation.addr, Size::from_usize(buffer.bytes().len()))
                 .copy_from_slice(buffer.bytes());

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
    memory: Memory<S>,
    record_id_free_list: Vec<RecordId>,
    records: Vec<Record>,
    buffer_providers: Vec<BufferProvider>,
}

impl<S: Storage> Database<S> {

    pub fn init(mut memory: Memory<S>) -> Database<S> {
        header::reserve_header(&mut memory);

        Database {
            memory,
            record_id_free_list: Vec::new(),
            records: Vec::new(),
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

    pub fn get_record(&self, record_id: RecordId) -> MemRef {
        let record = &self.records[record_id.idx()];
        self.memory.get_bytes(record.addr, record.size)
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
        self.memory.free(Allocation::new(record.addr, record.size));
        self.records[record_id.idx()] = Record::null();
        self.record_id_free_list.push(record_id);
    }

    pub fn persist(self) {
        mem::drop(self);
    }
}

impl<S: Storage> Drop for Database<S> {
    fn drop(&mut self) {
        if S::IS_READONLY {
            return
        }

        // let record_table_addr = record::persist_record_table(&self.memory,
        //                                                      self.records,
        //                                                      self.record_id_free_list);

        // Find footer address
        let footer_addr = self.memory.allocator.lock().max_addr();

        // Write footer
        // footer::write_footer(&mut self.memory.storage, footer_addr, &self.memory.allocator);

        header::write_header(&mut self.memory.storage, false, footer_addr);
    }
}





