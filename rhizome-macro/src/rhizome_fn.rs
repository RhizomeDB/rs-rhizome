use proc_macro2::TokenStream;
use quote::{quote, ToTokens};
use syn::{
    parenthesized,
    parse::{Parse, ParseStream},
    punctuated::Punctuated,
    Attribute, Generics, Ident, Token, Type,
};

pub(crate) fn expand(input: RhizomeFunctionDecl) -> TokenStream {
    let RhizomeFunctionDecl {
        mut attributes,
        generics,
        fn_token,
        fn_name,
        args,
        return_type,
    } = input;

    let is_aggregate = attributes
        .iter()
        .find(|attr| attr.meta.path().is_ident("aggregate"))
        .cloned();

    let is_predicate = attributes
        .iter()
        .find(|attr| attr.meta.path().is_ident("predicate"))
        .cloned();

    attributes.retain(|attr| !attr.meta.path().is_ident("aggregate"));
    attributes.retain(|attr| !attr.meta.path().is_ident("predicate"));

    let (ref arg_name, ref arg_type): (Vec<_>, Vec<_>) =
        args.iter().map(|arg| (&arg.name, &arg.ty)).unzip();

    let arg_struct_assign = args.iter().map(
        |StrictFnArg {
             name, colon_token, ..
         }| {
            let value = name.clone();

            quote!(#name #colon_token #value.into())
        },
    );

    let type_args = &generics
        .type_params()
        .map(|param| param.ident.clone())
        .collect::<Vec<_>>();

    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();
    let where_clause = where_clause
        .map(|w| quote!(#w))
        .unwrap_or_else(|| quote!(where));

    let mut tokens = quote! {
        use rhizome::var::Var;

        use super::*;

        #[derive(Debug, Clone, Copy)]
        pub struct #fn_name #ty_generics {
            #(pub #arg_name: Var,)*
            #(pub #type_args: ::std::marker::PhantomData<#type_args>,)*
        }

        pub type HelperType #ty_generics = #fn_name <
            #(#type_args,)*
        >;
    };

    let input_type = if arg_name.is_empty() {
        quote!((),)
    } else {
        quote!((#(#arg_type,)*),)
    };

    if let Some(agg_expr) = is_aggregate {
        let agg_expr = agg_expr.meta.require_name_value().unwrap().value.clone();

        tokens = quote! {
            #tokens

            impl #impl_generics ::rhizome::aggregation::AggregateGroupBy<#input_type #return_type> for HelperType #ty_generics
            #where_clause
            {
                type Aggregate = #agg_expr<#return_type>;

                fn as_args(&self) -> Vec<Var> {
                    let mut result = Vec::default();

                    #(
                        result.push(self.#arg_name);
                    )*

                    result
                }
            }
        }
    } else if let Some(pred_expr) = is_predicate {
        let pred_expr = pred_expr.meta.require_name_value().unwrap().value.clone();

        tokens = quote! {
            #tokens

            impl #impl_generics ::rhizome::predicate::PredicateWhere<#input_type> for HelperType #ty_generics
            #where_clause
            {
                type Predicate = #pred_expr<(#(#arg_type,)*)>;

                fn into_predicate(self) -> Self::Predicate {
                    Self::Predicate::default()
                }

                fn as_args(&self) -> Vec<Var> {
                    let mut result = Vec::default();

                    #(
                        result.push(self.#arg_name);
                    )*

                    result
                }
            }
        }
    }

    let arg_vars = args.iter().map(
        |StrictFnArg {
             name,
             colon_token,
             ty,
         }| { quote!(#name #colon_token ::rhizome::var::TypedVar<#ty>) },
    );

    quote! {
        #(#attributes)*
        #[allow(non_camel_case_types)]
        pub #fn_token #fn_name #impl_generics (#(#arg_vars,)*)
            -> #fn_name::HelperType #ty_generics
        #where_clause
        {
            #fn_name::#fn_name {
                #(#arg_struct_assign,)*
                #(#type_args: ::std::marker::PhantomData,)*
            }
        }

        #[doc(hidden)]
        #[allow(non_camel_case_types, non_snake_case, unused_imports)]
        pub(crate) mod #fn_name {
            #tokens
        }
    }
}

pub(crate) struct RhizomeFunctionDecl {
    attributes: Vec<Attribute>,
    fn_token: Token![fn],
    fn_name: Ident,
    generics: Generics,
    args: Punctuated<StrictFnArg, Token![,]>,
    return_type: Option<Type>,
}

struct StrictFnArg {
    name: Ident,
    colon_token: Token![:],
    ty: Type,
}

impl Parse for RhizomeFunctionDecl {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let attributes = Attribute::parse_outer(input)?;
        let fn_token: Token![fn] = input.parse()?;
        let fn_name = Ident::parse(input)?;
        let generics = Generics::parse(input)?;

        let args;
        parenthesized!(args in input);
        let args = args.parse_terminated(StrictFnArg::parse, Token![,])?;

        let return_type = if Option::<Token![->]>::parse(input)?.is_some() {
            Some(Type::parse(input)?)
        } else {
            None
        };

        input.parse::<Token![;]>()?;

        Ok(Self {
            attributes,
            fn_token,
            fn_name,
            generics,
            args,
            return_type,
        })
    }
}

impl Parse for StrictFnArg {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let name = input.parse()?;
        let colon_token = input.parse()?;
        let ty = input.parse()?;

        Ok(Self {
            name,
            colon_token,
            ty,
        })
    }
}

impl ToTokens for StrictFnArg {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        self.name.to_tokens(tokens);
        self.colon_token.to_tokens(tokens);
        self.name.to_tokens(tokens);
    }
}
