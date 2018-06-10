
use address::Size;

pub struct Buffer<'data> {
    data: &'data mut Vec<u8>,
    start: usize,
}

pub struct BufferProvider {
    data: Vec<u8>,
}

impl BufferProvider {

     pub fn new() -> BufferProvider {
        BufferProvider {
            data: Vec::new(),
        }
    }

    pub fn get_buffer(&mut self) -> Buffer {
        Buffer {
            data: &mut self.data,
            start: 0,
        }
    }
}

impl<'data> Buffer<'data> {
    #[inline(always)]
    pub fn write_byte(&mut self, byte: u8) {
        self.data.push(byte);
    }

    #[inline(always)]
    pub fn write_bytes(&mut self, bytes: &[u8]) {
        self.data.extend_from_slice(bytes);
    }

    #[inline(always)]
    pub fn len(&self) -> Size {
        Size(self.data.len() - self.start)
    }

    pub fn start_sub_buffer<'s>(&'s mut self) -> Buffer<'s>
        where 'data: 's
    {
        let start = self.data.len();

        Buffer {
            data: self.data,
            start,
        }
    }

    pub fn bytes(&self) -> &[u8] {
        &self.data[self.start ..]
    }
}

impl<'data> Drop for Buffer<'data> {
    fn drop(&mut self) {
        self.data.truncate(self.start);
    }
}


