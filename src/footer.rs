
use memory::*;
use persist::*;
use allocator::*;

// const FOOTER_MAGIC: [u8; 4] = [b'D', b'I', b'B', b'S'];

// pub fn write_footer<S: Storage>(storage: &mut S,
//                                 addr: Address,
//                                 allocator: &Allocator) {
//     // Write footer magic
//     storage.get_bytes_mut_exclusive(addr, Size::from_usize(FOOTER_MAGIC.len()))
//            .copy_from_slice(&FOOTER_MAGIC);

//     let mut writer = StorageWriter::new(storage, addr + Size::from_usize(FOOTER_MAGIC.len()));

//     // Write allocator
//     allocator.write(&mut writer);

//     // Write record index
// }
