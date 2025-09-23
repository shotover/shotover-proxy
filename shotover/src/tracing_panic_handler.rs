use backtrace::{Backtrace, BacktraceFmt, BytesOrWideString, PrintFmt};
use std::fmt;

pub fn setup() {
    std::panic::set_hook(Box::new(|panic| {
        let backtrace = BacktraceFormatter(Backtrace::new());
        // If the panic has a source location, record it as structured fields.
        if let Some(location) = panic.location() {
            tracing::error!(
                message = %panic,
                panic.file = location.file(),
                panic.line = location.line(),
                panic.column = location.column(),
                panic.backtrace = format!("{backtrace}"),
            );
        } else {
            tracing::error!(
                message = %panic,
                panic.backtrace = format!("{backtrace}"),
            );
        }
    }));
}

/// The std::backtrace::Backtrace formatting is really noisy because it includes all the pre-main and panic handling frames.
/// Internal panics have logic to remove that but that is missing from std::backtrace::Backtrace.
/// https://github.com/rust-lang/rust/issues/105413
///
/// As a workaround we use the backtrace crate and manually perform the required formatting
struct BacktraceFormatter(Backtrace);

// based on https://github.com/rust-lang/backtrace-rs/blob/5be2e8ba9cf6e391c5fa45219fc091b4075eb6be/src/capture.rs#L371
impl fmt::Display for BacktraceFormatter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // When printing paths we try to strip the cwd if it exists, otherwise
        // we just print the path as-is. Note that we also only do this for the
        // short format, because if it's full we presumably want to print
        // everything.
        let cwd = std::env::current_dir();
        let mut print_path = move |fmt: &mut fmt::Formatter<'_>, path: BytesOrWideString<'_>| {
            let path = path.into_path_buf();
            if let Ok(cwd) = &cwd
                && let Ok(suffix) = path.strip_prefix(cwd)
            {
                return fmt::Display::fmt(&suffix.display(), fmt);
            }
            fmt::Display::fmt(&path.display(), fmt)
        };

        let mut f = BacktraceFmt::new(f, PrintFmt::Short, &mut print_path);
        f.add_context()?;
        for (frame, _) in backtrace_ext::short_frames_strict(&self.0) {
            f.frame().backtrace_frame(frame)?;
        }
        f.finish()
    }
}
