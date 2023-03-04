use pretty::RcDoc;

use crate::{
    id::LinkId,
    pretty::Pretty,
    ram::{formula::Formula, term::Term},
};

use super::Operation;

#[derive(Clone, Debug)]
pub struct GetLink {
    cid_term: Term,
    // TODO: It may be worthwhile to allow the LinkId to be unbound to support iterating through a CID's links,
    // but I'm not sure yet whether that has has implications on termination or convergence. It shouldn't,
    // since only EDB facts have links, and so the universe of LinkIds is finite, but we can keep things
    // simple to start.
    link_id: LinkId,
    link_value: Term,
    when: Vec<Formula>,
    operation: Box<Operation>,
}

impl GetLink {
    pub fn new(
        cid_term: Term,
        link_id: LinkId,
        link_value: Term,
        when: impl IntoIterator<Item = Formula>,
        operation: Operation,
    ) -> Self {
        let when = when.into_iter().collect();

        Self {
            cid_term,
            link_id,
            link_value,
            when,
            operation: Box::new(operation),
        }
    }

    pub fn cid_term(&self) -> &Term {
        &self.cid_term
    }

    pub fn link_id(&self) -> &LinkId {
        &self.link_id
    }

    pub fn link_value(&self) -> &Term {
        &self.link_value
    }

    pub fn when(&self) -> &Vec<Formula> {
        &self.when
    }

    pub fn operation(&self) -> &Operation {
        &self.operation
    }
}

impl Pretty for GetLink {
    fn to_doc(&self) -> RcDoc<'_, ()> {
        RcDoc::concat([
            RcDoc::text("get_link("),
            RcDoc::intersperse(
                [
                    self.cid_term().to_doc(),
                    RcDoc::as_string(self.link_id()),
                    self.link_value().to_doc(),
                ],
                RcDoc::text(","),
            ),
            RcDoc::text(") do"),
        ])
        .append(
            RcDoc::hardline()
                .append(self.operation().to_doc())
                .nest(2)
                .group(),
        )
    }
}
