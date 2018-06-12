
use std::ops::{Add, AddAssign, Sub};

#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct Address(pub u32);

#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct Size(pub u32);

impl Add<Size> for Address {
    type Output = Address;
    fn add(self, rhs: Size) -> Self::Output {
        Address(self.0 + rhs.0)
    }
}

impl Add<Size> for Size {
    type Output = Size;
    fn add(self, rhs: Size) -> Self::Output {
        Size(self.0 + rhs.0)
    }
}

impl Sub<Size> for Size {
    type Output = Size;
    fn sub(self, rhs: Size) -> Self::Output {
        Size(self.0 - rhs.0)
    }
}

impl AddAssign<Size> for Size {
    fn add_assign(&mut self, rhs: Size) {
        self.0 += rhs.0;
    }
}

impl Size {
    pub fn from_usize(x: usize) -> Size {
        let size = Size(x as u32);
        assert!(size.0 as usize == x);
        size
    }
}
