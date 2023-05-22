use crate::{col_val::ColVal, id::ColId};

use super::atom_binding::AtomBinding;

pub trait AtomBindings {
    fn bind(self, bindings: &mut Vec<(ColId, ColVal)>);
}

macro_rules! impl_atom_bindings {
    ($($Bs:expr),*) => {
        paste::item! {
            impl<$([< B $Bs >],)*> AtomBindings for ($([< B $Bs >],)*)
            where
                $([< B $Bs >]: AtomBinding,)*
            {
                #[allow(unused_variables)]
                fn bind(self, bindings: &mut Vec<(ColId, ColVal)>) {

                    $(bindings.push(self.[< $Bs >].into_pair());)*
                }
            }
        }
    };
}

impl_atom_bindings!();
impl_atom_bindings!(0);
impl_atom_bindings!(0, 1);
impl_atom_bindings!(0, 1, 2);
impl_atom_bindings!(0, 1, 2, 3);
impl_atom_bindings!(0, 1, 2, 3, 4);
impl_atom_bindings!(0, 1, 2, 3, 4, 5);
impl_atom_bindings!(0, 1, 2, 3, 4, 5, 6);
impl_atom_bindings!(0, 1, 2, 3, 4, 5, 6, 7);
