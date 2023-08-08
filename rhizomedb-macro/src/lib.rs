use proc_macro::TokenStream;
use syn::parse_macro_input;

mod rhizome_fn;

#[proc_macro]
pub fn rhizome_fn(input: TokenStream) -> TokenStream {
    rhizome_fn::expand(parse_macro_input!(input)).into()
}
