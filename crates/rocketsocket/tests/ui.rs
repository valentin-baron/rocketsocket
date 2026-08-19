//! Compile-fail tests for `#[event]`.
//!
//! The error messages are the developer experience: a macro that rejects a handler with an
//! unintelligible span is worse than no macro. These pin the wording.

#[test]
fn event_macro_rejects_bad_handlers_clearly() {
    trybuild::TestCases::new().compile_fail("tests/ui/*.rs");
}
