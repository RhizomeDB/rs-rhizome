use std::cmp::Ordering;

pub trait Lattice {
    const BOTTOM: Self;
    const TOP: Self;

    fn cmp(&self, other: &Self) -> Option<Ordering>;

    fn join(&self, other: &Self) -> Self;
    fn meet(&self, other: &Self) -> Self;
}

impl<A, B> Lattice for (A, B)
where
    A: Lattice,
    B: Lattice,
{
    const BOTTOM: Self = (A::BOTTOM, B::BOTTOM);
    const TOP: Self = (A::TOP, B::TOP);

    fn cmp(&self, other: &Self) -> Option<Ordering> {
        match (A::cmp(&self.0, &other.0), B::cmp(&self.1, &other.1)) {
            (None, _) => None,
            (_, None) => None,
            (Some(a), Some(b)) if a == b => Some(a),
            (Some(a), Some(Ordering::Equal)) => Some(a),
            (Some(Ordering::Equal), Some(b)) => Some(b),
            _ => None,
        }
    }

    fn join(&self, other: &Self) -> Self {
        (A::join(&self.0, &other.0), B::join(&self.1, &other.1))
    }

    fn meet(&self, other: &Self) -> Self {
        (A::meet(&self.0, &other.0), B::meet(&self.1, &other.1))
    }
}

macro_rules! impl_lattice {
    ($type:ty, $bottom:expr, $top:expr) => {
        impl Lattice for $type {
            const BOTTOM: Self = $bottom;
            const TOP: Self = $top;

            #[inline]
            fn cmp(&self, other: &Self) -> Option<Ordering> {
                Some(Ord::cmp(self, other))
            }

            fn join(&self, other: &Self) -> Self {
                Ord::max(*self, *other)
            }

            fn meet(&self, other: &Self) -> Self {
                Ord::min(*self, *other)
            }
        }
    };
}

impl_lattice!((), (), ());
impl_lattice!(usize, 0, usize::MAX);
impl_lattice!(isize, isize::MIN, isize::MAX);
impl_lattice!(u8, 0, u8::MAX);
impl_lattice!(i8, i8::MIN, i8::MAX);
impl_lattice!(u16, 0, u16::MAX);
impl_lattice!(i16, i16::MIN, i16::MAX);
impl_lattice!(u32, 0, u32::MAX);
impl_lattice!(i32, i32::MIN, i32::MAX);
impl_lattice!(u64, 0, u64::MAX);
impl_lattice!(i64, i64::MIN, i64::MAX);
impl_lattice!(u128, 0, u128::MAX);
impl_lattice!(i128, i128::MIN, i128::MAX);

#[cfg(test)]
mod tests {
    use super::*;

    macro_rules! test_numeric_lattice {
        ($type:ty, $bottom:expr, $top:expr) => {
            paste::item! {
                #[test]
                fn [< $type _lattice_tests >] () {
                    assert_eq!($bottom, <$type as Lattice>::BOTTOM);
                    assert_eq!($top, <$type as Lattice>::TOP);

                    assert_eq!(Some(Ordering::Less), <$type as Lattice>::cmp(&$bottom, &$top));
                    assert_eq!(Some(Ordering::Greater), <$type as Lattice>::cmp(&$top, &$bottom));
                    assert_eq!(Some(Ordering::Equal), <$type as Lattice>::cmp(&$bottom, &$bottom));
                    assert_eq!(Some(Ordering::Equal), <$type as Lattice>::cmp(&$top, &$top));

                    assert_eq!($top, <$type as Lattice>::join(&$top, &$top));
                    assert_eq!($top, <$type as Lattice>::join(&$bottom, &$top));
                    assert_eq!($top, <$type as Lattice>::join(&$top, &$bottom));
                    assert_eq!($bottom, <$type as Lattice>::join(&$bottom, &$bottom));

                    assert_eq!($top, <$type as Lattice>::meet(&$top, &$top));
                    assert_eq!($bottom, <$type as Lattice>::meet(&$top, &$bottom));
                    assert_eq!($bottom, <$type as Lattice>::meet(&$bottom, &$top));
                    assert_eq!($bottom, <$type as Lattice>::meet(&$bottom, &$bottom));
                }
            }
        };
    }

    #[test]
    fn unit_lattice_tests() {
        assert_eq!((), <()>::BOTTOM);
        assert_eq!((), <()>::TOP);

        assert_eq!(Some(Ordering::Equal), Lattice::cmp(&(), &()));

        assert_eq!((), Lattice::join(&(), &()));
        assert_eq!((), Lattice::meet(&(), &()));
    }

    #[test]
    fn pair_lattice_tests() {
        assert_eq!((0, 0), <(usize, usize)>::BOTTOM);
        assert_eq!((usize::MAX, usize::MAX), <(usize, usize)>::TOP);

        assert_eq!(None, Lattice::cmp(&(1, 2), &(2, 1)));
        assert_eq!(Some(Ordering::Less), Lattice::cmp(&(1, 2), &(2, 3)));
        assert_eq!(Some(Ordering::Less), Lattice::cmp(&(1, 2), &(1, 3)));
        assert_eq!(Some(Ordering::Less), Lattice::cmp(&(1, 2), &(2, 2)));
        assert_eq!(Some(Ordering::Greater), Lattice::cmp(&(1, 2), &(0, 1)));
        assert_eq!(Some(Ordering::Greater), Lattice::cmp(&(1, 2), &(1, 1)));
        assert_eq!(Some(Ordering::Greater), Lattice::cmp(&(1, 2), &(0, 2)));
        assert_eq!(Some(Ordering::Equal), Lattice::cmp(&(1, 2), &(1, 2)));

        assert_eq!((3, 4), Lattice::join(&(3, 2), &(1, 4)));
        assert_eq!((3, 4), Lattice::join(&(1, 4), &(3, 2)));

        assert_eq!((1, 2), Lattice::meet(&(3, 2), &(1, 4)));
        assert_eq!((1, 2), Lattice::meet(&(1, 4), &(3, 2)));
    }

    test_numeric_lattice!(usize, 0, usize::MAX);
    test_numeric_lattice!(isize, isize::MIN, isize::MAX);
    test_numeric_lattice!(u8, 0, u8::MAX);
    test_numeric_lattice!(i8, i8::MIN, i8::MAX);
    test_numeric_lattice!(u16, 0, u16::MAX);
    test_numeric_lattice!(i16, i16::MIN, i16::MAX);
    test_numeric_lattice!(u32, 0, u32::MAX);
    test_numeric_lattice!(i32, i32::MIN, i32::MAX);
    test_numeric_lattice!(u64, 0, u64::MAX);
    test_numeric_lattice!(i64, i64::MIN, i64::MAX);
    test_numeric_lattice!(u128, 0, u128::MAX);
    test_numeric_lattice!(i128, i128::MIN, i128::MAX);
}
