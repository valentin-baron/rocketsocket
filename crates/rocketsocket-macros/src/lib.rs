//! Proc macros for [`rocketsocket`](https://docs.rs/rocketsocket).
//!
//! Only [`macro@event`] lives here. There is deliberately no `#[command]`: Rocket.Chat
//! gives an external bot no way to register a slash command, no autocomplete and no
//! description surface, so a command macro would be sugar over splitting a string —
//! see `docs/dx.md` §7.
//!
//! # Layering
//!
//! Everything this macro produces is a plain value you could have written by hand. It
//! expands to a function returning a handler, so registration is explicit and inspectable
//! rather than a side effect, and the raw event stream stays a supported API. That is what
//! keeps the framework optional rather than load-bearing.

use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::spanned::Spanned;
use syn::{FnArg, ItemFn, PatType, ReturnType, Type, parse_macro_input};

/// Declares an event handler.
///
/// The event is identified by the **parameter type**, not the function name:
///
/// ```ignore
/// #[rocketsocket::event]
/// async fn greet(ctx: Context<Data>, message: MessageCreate) -> Result<()> {
///     message.reply("hi").await
/// }
/// ```
///
/// discord.py dispatches on the function name (`on_message`), which ported literally to
/// Rust would turn a typo into a handler that compiles, registers, and silently never
/// fires. Keying on the type makes it a compile error instead, and frees the name — so two
/// handlers for the same event no longer collide.
///
/// Exactly one parameter must be the event. Every other parameter is an extractor, so a
/// handler declares only what it needs.
#[proc_macro_attribute]
pub fn event(args: TokenStream, input: TokenStream) -> TokenStream {
    if !args.is_empty() {
        let span = proc_macro2::TokenStream::from(args).span();
        return syn::Error::new(
            span,
            "`#[event]` takes no arguments; the event is identified by the handler's \
             parameter type",
        )
        .to_compile_error()
        .into();
    }

    let function = parse_macro_input!(input as ItemFn);
    match expand(function) {
        Ok(tokens) => tokens.into(),
        Err(error) => error.to_compile_error().into(),
    }
}

fn expand(function: ItemFn) -> syn::Result<proc_macro2::TokenStream> {
    if function.sig.asyncness.is_none() {
        return Err(syn::Error::new(
            function.sig.fn_token.span(),
            "an event handler must be `async`",
        ));
    }

    if !function.sig.generics.params.is_empty() {
        return Err(syn::Error::new(
            function.sig.generics.span(),
            "an event handler cannot be generic: it is stored as a value, so its type must \
             be known at registration",
        ));
    }

    if matches!(function.sig.output, ReturnType::Default) {
        return Err(syn::Error::new(
            function.sig.span(),
            "an event handler must return `Result<(), E>`, so a failure is reported rather \
             than swallowed",
        ));
    }

    let parameters = collect_parameters(&function)?;
    if parameters.is_empty() {
        return Err(syn::Error::new(
            function.sig.inputs.span(),
            "an event handler needs at least one parameter: the event it handles",
        ));
    }

    let name = &function.sig.ident;
    let visibility = &function.vis;
    let attributes = &function.attrs;
    let inner = format_ident!("__rocketsocket_inner_{}", name);

    // The user's body moves verbatim into an inner fn, so diagnostics inside it point at
    // the code they wrote rather than at macro output. Getting this wrong is the single
    // biggest reason proc macros are unpleasant to use.
    let mut body = function.clone();
    body.sig.ident = inner.clone();
    body.attrs.clear();
    body.vis = syn::Visibility::Inherited;

    let types: Vec<&Type> = parameters.iter().map(|parameter| &*parameter.ty).collect();
    let bindings: Vec<_> = (0..types.len()).map(|index| format_ident!("__arg{index}")).collect();
    let data = infer_data_type(&types)?;

    Ok(quote! {
        #(#attributes)*
        #visibility fn #name() -> ::rocketsocket::framework::Handler<#data> {
            #body

            ::rocketsocket::framework::Handler::new(
                ::std::stringify!(#name),
                |__event, __context| {
                    let __context = ::std::clone::Clone::clone(__context);
                    let __extracted =
                        <(#(#types,)*) as ::rocketsocket::framework::HandlerArgs<#data>>::extract(
                            __event, &__context,
                        );
                    ::std::boxed::Box::pin(async move {
                        // A `Skip` is the ordinary case -- the handler declined an event it
                        // does not handle -- so it must not be reported as a failure.
                        let (#(#bindings,)*) = match __extracted {
                            ::std::result::Result::Ok(args) => args,
                            // `Extract` is non-exhaustive, so only `Skip` is matched by
                            // name. Anything else is reported rather than silently
                            // treated as "not my event" -- a future variant that means
                            // "failed" must not vanish.
                            ::std::result::Result::Err(
                                ::rocketsocket::framework::Extract::Skip
                            ) => return ::std::result::Result::Ok(()),
                            ::std::result::Result::Err(__reason) => {
                                return ::std::result::Result::Err(
                                    ::rocketsocket::framework::HandlerError::extraction(
                                        ::std::format!("{__reason:?}"),
                                    ),
                                );
                            }
                        };
                        #inner(#(#bindings),*)
                            .await
                            .map_err(::rocketsocket::framework::HandlerError::from_handler)
                    })
                },
            )
        }
    })
}

/// Finds the bot's data type from a `Context<D>` or `State<D>` parameter.
///
/// The generated function has to name `Handler<D>` concretely, and the only place `D`
/// appears is in the handler's own signature. Poise does the same thing with its
/// `Context<'_, U, E>`.
fn infer_data_type(types: &[&Type]) -> syn::Result<proc_macro2::TokenStream> {
    for ty in types {
        if let Type::Path(path) = ty
            && let Some(segment) = path.path.segments.last()
            && (segment.ident == "Context" || segment.ident == "State")
            && let syn::PathArguments::AngleBracketed(arguments) = &segment.arguments
            && let Some(syn::GenericArgument::Type(data)) = arguments.args.first()
        {
            return Ok(quote! { #data });
        }
    }

    Err(syn::Error::new(
        proc_macro2::Span::call_site(),
        "an event handler must take a `Context<D>` or `State<D>` parameter, so the macro \
         can name the bot's data type in the handler it returns",
    ))
}

/// The handler's parameters, rejecting `self` with a useful message.
fn collect_parameters(function: &ItemFn) -> syn::Result<Vec<&PatType>> {
    function
        .sig
        .inputs
        .iter()
        .map(|argument| match argument {
            // Any pattern is fine, including `State(data): State<Data>` — the extractor
            // is chosen by the *type*, which is always written out, so destructuring
            // hides nothing. The pattern stays on the inner fn and never appears here.
            FnArg::Typed(typed) => Ok(typed),
            FnArg::Receiver(receiver) => Err(syn::Error::new(
                receiver.span(),
                "an event handler is a free function, not a method",
            )),
        })
        .collect()
}
