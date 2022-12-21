use pretty::RcDoc;

pub trait Pretty {
    fn to_doc(&self) -> RcDoc<'_, ()>;
}
