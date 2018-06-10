
use std::ops::{Add, AddAssign, Sub};

#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct Address(pub usize);

#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct Size(pub usize);

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
