
use std::mem;
use memory::{Memory, Storage, Size, Address};
use byteorder::{ByteOrder, LittleEndian};

const FILE_MAGIC: [u8; 4] = [b'D', b'I', b'B', b's'];

const FILE_FORMAT_VERSION: u32 = 1;

bitflags! {
    struct Flags: u32 {
        const SUPPORTS_GC = 0b00000001;
    }
}

#[repr(C, packed)]
pub struct Header {
    file_magic: [u8; 4],
    file_format_version: u32,
    flags: Flags,
    footer_addr: Address,
}

pub fn read_header<S: Storage>(storage: &S) -> Result<Header, String> {
    if storage.size() < Size::from_usize(mem::size_of::<Header>()) {
        return Err("File too small".to_string());
    }

    let header_bytes = unsafe {
        storage.get_bytes(Address(0), Size::from_usize(mem::size_of::<Header>()))
    };

    if &header_bytes[0 .. 4] != FILE_MAGIC {
        return Err(format!("File magic does not match."));
    }

    let file_format_version = LittleEndian::read_u32(&header_bytes[4 .. 8]);

    if file_format_version != FILE_FORMAT_VERSION {
        return Err(format!("Invalid file format version. Expected {}, was {}.",
                           FILE_FORMAT_VERSION,
                           file_format_version));
    }

    let flags = LittleEndian::read_u32(&header_bytes[8 .. 12]);
    let flags = if let Some(flags) = Flags::from_bits(flags) {
        flags
    } else {
        return Err(format!("Header contains invalid flags field: {:b}", flags));
    };

    let footer_addr = Address(LittleEndian::read_u32(&header_bytes[12 .. 16]));

    if footer_addr >= Address::from_u32(0) + storage.size() {
        return Err(format!("File footer addr outside of file"));
    }

    let header = Header {
        file_magic: FILE_MAGIC,
        file_format_version,
        footer_addr,
        flags,
    };

    Ok(header)
}

pub fn write_header<S: Storage>(storage: &S,
                                supports_gc: bool,
                                footer_addr: Address) {
    let mut flags = Flags::empty();

    if supports_gc {
        flags |= Flags::SUPPORTS_GC;
    }

    let header_bytes = unsafe {
        storage.get_bytes_mut(Address(0), Size::from_usize(mem::size_of::<Header>()))
    };

    header_bytes[0..4].copy_from_slice(&FILE_MAGIC);
    LittleEndian::write_u32(&mut header_bytes[ 4 ..  8], FILE_FORMAT_VERSION);
    LittleEndian::write_u32(&mut header_bytes[ 8 .. 12], flags.bits());
    LittleEndian::write_u32(&mut header_bytes[12 .. 16], footer_addr.as_u32());
}

pub fn reserve_header<S: Storage>(memory: &mut Memory<S>) {
    let header_size = Size::from_usize(mem::size_of::<Header>());
    let alloc = memory.alloc(header_size);
    assert_eq!(alloc.addr, Address::from_u32(0));
    assert_eq!(alloc.size, header_size);
}
