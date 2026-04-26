mod admin;
mod events;
mod files;
mod io;
mod mutation;
mod namespace;
mod search;

pub(crate) use admin::AdminSvc;
pub(crate) use events::EventsSvc;
pub(crate) use files::FilesSvc;
pub(crate) use io::IoSvc;
pub(crate) use mutation::MutationSvc;
pub(crate) use namespace::NamespaceSvc;
pub(crate) use search::SearchSvc;
