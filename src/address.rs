
use std::ops::{Add, AddAssign, Sub, Mul};

#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct Address(pub u32);

#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct Size(pub u32);

impl Add<Size> for Address {
    type Output = Address;

    #[inline]
    fn add(self, rhs: Size) -> Self::Output {
        Address(self.0 + rhs.0)
    }
}

impl Add<Size> for Size {
    type Output = Size;

    #[inline]
    fn add(self, rhs: Size) -> Self::Output {
        Size(self.0 + rhs.0)
    }
}

impl Sub<Size> for Size {
    type Output = Size;

    #[inline]
    fn sub(self, rhs: Size) -> Self::Output {
        Size(self.0 - rhs.0)
    }
}

impl Mul<Size> for Size {
    type Output = Size;

    #[inline]
    fn mul(self, rhs: Size) -> Self::Output {
        Size(self.0 * rhs.0)
    }
}

impl Mul<usize> for Size {
    type Output = Size;

    #[inline]
    fn mul(self, rhs: usize) -> Self::Output {
        Size(self.0 * rhs as u32)
    }
}

impl Mul<u32> for Size {
    type Output = Size;

    #[inline]
    fn mul(self, rhs: u32) -> Self::Output {
        Size(self.0 * rhs)
    }
}

impl AddAssign<Size> for Size {
    #[inline]
    fn add_assign(&mut self, rhs: Size) {
        self.0 += rhs.0;
    }
}

impl Address {
    #[inline]
    pub fn from_usize(x: usize) -> Address {
        let addr = Address(x as u32);
        assert!(addr.0 as usize == x);
        addr
    }
}

impl Size {
    #[inline]
    pub fn from_usize(x: usize) -> Size {
        let size = Size(x as u32);
        assert!(size.0 as usize == x);
        size
    }
}
