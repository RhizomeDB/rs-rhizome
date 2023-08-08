#[cfg(not(target_arch = "wasm32"))]
#[path = "tokio.rs"]
mod imp;
#[cfg(target_arch = "wasm32")]
#[path = "wasm.rs"]
mod imp;

#[cfg(not(target_arch = "wasm32"))]
pub trait MaybeSend: Send {}
#[cfg(not(target_arch = "wasm32"))]
impl<T: Send> MaybeSend for T {}

#[cfg(target_arch = "wasm32")]
pub trait MaybeSend {}
#[cfg(target_arch = "wasm32")]
impl<T> MaybeSend for T {}

pub use imp::*;
