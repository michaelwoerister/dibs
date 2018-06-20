
use byteorder::{ByteOrder, LittleEndian};
use memory::*;

pub struct StorageWriter<'s, S: Storage + 's> {
    storage: &'s Memory<S>,
    addr: Address,
}

impl<'s, S: Storage + 's> StorageWriter<'s, S> {

    #[inline]
    pub fn new(storage: &'s Memory<S>, addr: Address) -> Self {
        StorageWriter {
            storage,
            addr,
        }
    }

    #[inline]
    pub fn write_u32(&mut self, val: u32) {
        LittleEndian::write_u32(&mut self.storage.get_bytes_mut(self.addr, Size(4)), val);
        self.addr += Size(4);
    }

    #[inline]
    pub fn write_u64(&mut self, val: u64) {
        LittleEndian::write_u64(&mut self.storage.get_bytes_mut(self.addr, Size(8)), val);
        self.addr += Size(8);
    }
}

pub trait Serialize {
    fn write<'s, S: Storage + 's>(&self, writer: &mut StorageWriter<'s, S>);

    #[inline]
    fn write_at<S: Storage>(&self, storage: &Memory<S>, addr: Address) {
        self.write(&mut StorageWriter::new(storage, addr));
    }
}

impl<T: Serialize> Serialize for Vec<T> {
    #[inline]
    fn write<'s, S: Storage + 's>(&self, writer: &mut StorageWriter<'s, S>) {
        Size::from_usize(self.len()).write(writer);

        for x in self.iter() {
            x.write(writer);
        }
    }
}

pub struct StorageReader<'s, S: Storage + 's> {
    storage: &'s Memory<S>,
    addr: Address,
}

impl<'s, S: Storage + 's> StorageReader<'s, S> {

    #[inline]
    pub fn new(storage: &'s Memory<S>, addr: Address) -> StorageReader<'s, S> {
        StorageReader {
            storage,
            addr,
        }
    }

    #[inline]
    pub fn read_u32(&mut self) -> u32 {
        let val = LittleEndian::read_u32(&self.storage.get_bytes(self.addr, Size(4)));
        self.addr += Size(4);
        val
    }

    #[inline]
    pub fn read_u64(&mut self) -> u64 {
        let val = LittleEndian::read_u64(&self.storage.get_bytes(self.addr, Size(8)));
        self.addr += Size(8);
        val
    }
}

pub trait Deserialize: Sized {
    fn read<'s, S: Storage + 's>(reader: &mut StorageReader<'s, S>) -> Self;

    #[inline]
    fn read_at<S: Storage>(storage: &Memory<S>, addr: Address) -> Self {
        Self::read(&mut StorageReader::new(storage, addr))
    }
}


impl Serialize for u32 {
    fn write<'s, S: Storage + 's>(&self, writer: &mut StorageWriter<'s, S>) {
        writer.write_u32(*self);
    }
}

impl Deserialize for u32 {
    fn read<'s, S: Storage + 's>(reader: &mut StorageReader<'s, S>) -> Self {
        reader.read_u32()
    }
}

impl Serialize for u64 {
    fn write<'s, S: Storage + 's>(&self, writer: &mut StorageWriter<'s, S>) {
        writer.write_u64(*self);
    }
}

impl Deserialize for u64 {
    fn read<'s, S: Storage + 's>(reader: &mut StorageReader<'s, S>) -> Self {
        reader.read_u64()
    }
}
