//! Regenerates the stream catalog in `rocketsocket-model`.
//!
//! ```text
//! cargo run -p rocketsocket-codegen            # rewrite the generated region
//! cargo run -p rocketsocket-codegen -- --check # fail if it is out of date
//! cargo run -p rocketsocket-codegen -- --report-only
//! ```

use std::path::PathBuf;
use std::process::ExitCode;

use rocketsocket_codegen::{
    Catalog, KeyPattern, UPSTREAM_COMMIT, UPSTREAM_PATH, UPSTREAM_VERSION, VENDORED_STREAMS_TS,
    parse, render_catalog, splice,
};

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let check = args.iter().any(|a| a == "--check");
    let report_only = args.iter().any(|a| a == "--report-only");
    if let Some(bad) = args.iter().find(|a| !matches!(a.as_str(), "--check" | "--report-only")) {
        eprintln!("unknown argument `{bad}`; expected --check or --report-only");
        return ExitCode::FAILURE;
    }

    let catalog = match parse(VENDORED_STREAMS_TS) {
        Ok(catalog) => catalog,
        Err(error) => {
            eprintln!("error: could not read the vendored stream catalog");
            eprintln!("{error}");
            eprintln!();
            eprintln!(
                "This is a hard stop on purpose. `{UPSTREAM_PATH}` changed shape, and \
                 guessing at the new one would silently drop streams from the generated \
                 catalog — which would mean a bot that never receives those events. Teach \
                 the parser the new shape, then regenerate."
            );
            return ExitCode::FAILURE;
        }
    };

    report(&catalog);
    if report_only {
        return ExitCode::SUCCESS;
    }

    let target = target_path();
    let existing = match std::fs::read_to_string(&target) {
        Ok(text) => text,
        Err(error) => {
            eprintln!("error: cannot read {}: {error}", target.display());
            return ExitCode::FAILURE;
        }
    };

    let updated = match splice(&existing, &render_catalog(&catalog)) {
        Ok(text) => text,
        Err(error) => {
            eprintln!("error: {error} ({})", target.display());
            return ExitCode::FAILURE;
        }
    };

    if updated == existing {
        println!("\n{} is up to date", target.display());
        return ExitCode::SUCCESS;
    }
    if check {
        eprintln!("\nerror: {} is out of date", target.display());
        eprintln!("run `cargo run -p rocketsocket-codegen` and commit the result");
        return ExitCode::FAILURE;
    }
    if let Err(error) = std::fs::write(&target, updated) {
        eprintln!("error: cannot write {}: {error}", target.display());
        return ExitCode::FAILURE;
    }
    println!("\nwrote {}", target.display());
    println!("run `cargo fmt --all` if the generated region needs reformatting");
    ExitCode::SUCCESS
}

/// The file whose generated region this tool owns.
fn target_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../rocketsocket-model/src/event.rs")
        .components()
        .collect()
}

/// Prints what was parsed, so drift against the pinned copy is visible in the build log.
fn report(catalog: &Catalog) {
    println!("rocketsocket-codegen");
    println!("  source   {UPSTREAM_PATH}");
    println!("  pinned   {UPSTREAM_COMMIT} ({UPSTREAM_VERSION})");
    println!(
        "  parsed   {} streams, {} events\n",
        catalog.streams.len(),
        catalog.event_count()
    );

    let width = catalog.streams.iter().map(|s| s.name.len()).max().unwrap_or(0);
    for stream in &catalog.streams {
        let composite = stream
            .events
            .iter()
            .filter(|e| matches!(e.key, KeyPattern::Suffix(_) | KeyPattern::Prefix(_)))
            .count();
        let free = stream.events.iter().filter(|e| matches!(e.key, KeyPattern::Any)).count();
        let arity: Vec<String> = stream
            .events
            .iter()
            .map(|e| {
                if e.args.variadic {
                    "*".to_owned()
                } else {
                    e.args.arities.iter().map(usize::to_string).collect::<Vec<_>>().join("/")
                }
            })
            .collect();
        println!(
            "  stream-{:<width$}  {:>2} events  ({composite} composite, {free} free)  arity {}",
            stream.name,
            stream.events.len(),
            arity.join(" "),
        );
    }
}
