use std::{cmp, ops::Add};

use num_traits::One;

pub fn count<Acc, Args>(acc: Acc, _: Args) -> Acc
where
    Acc: Add<Output = Acc> + One,
{
    acc + One::one()
}

pub fn sum<Acc, Arg>(acc: Acc, arg: Arg) -> Acc
where
    Acc: Add<Arg, Output = Acc>,
{
    acc + arg
}

pub fn min<Arg>(acc: Arg, arg: Arg) -> Arg
where
    Arg: Ord,
{
    cmp::min(acc, arg)
}

pub fn max<Arg>(acc: Arg, arg: Arg) -> Arg
where
    Arg: Ord,
{
    cmp::max(acc, arg)
}
