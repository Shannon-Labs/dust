use std::env;
use std::fmt::Display;
use std::io::{self, IsTerminal};

#[derive(Debug, Clone, Copy)]
pub struct Palette {
    enabled: bool,
}

impl Palette {
    pub fn stdout() -> Self {
        Self {
            enabled: color_enabled(io::stdout().is_terminal()),
        }
    }

    pub fn title(&self, text: impl Display) -> String {
        paint(self.enabled, text, &["1", "36"])
    }

    pub fn header(&self, text: impl Display) -> String {
        self.title(text)
    }

    pub fn section(&self, text: impl Display) -> String {
        paint(self.enabled, text, &["1", "34"])
    }

    pub fn label(&self, text: impl Display) -> String {
        paint(self.enabled, text, &["1", "96"])
    }

    pub fn info(&self, text: impl Display) -> String {
        paint(self.enabled, text, &["36"])
    }

    pub fn success(&self, text: impl Display) -> String {
        paint(self.enabled, text, &["1", "32"])
    }

    pub fn error(&self, text: impl Display) -> String {
        paint(self.enabled, text, &["1", "31"])
    }

    pub fn warning(&self, text: impl Display) -> String {
        paint(self.enabled, text, &["1", "33"])
    }

    pub fn metric(&self, text: impl Display) -> String {
        paint(self.enabled, text, &["1", "35"])
    }

    pub fn command(&self, text: impl Display) -> String {
        paint(self.enabled, text, &["32"])
    }

    pub fn path(&self, text: impl Display) -> String {
        paint(self.enabled, text, &["36"])
    }

    pub fn muted(&self, text: impl Display) -> String {
        paint(self.enabled, text, &["2"])
    }

    pub fn dim(&self, text: impl Display) -> String {
        self.muted(text)
    }

    pub fn added(&self, text: impl Display) -> String {
        format!(
            "{} {}",
            paint(self.enabled, "+", &["1", "32"]),
            self.success(text)
        )
    }

    pub fn removed(&self, text: impl Display) -> String {
        format!(
            "{} {}",
            paint(self.enabled, "-", &["1", "31"]),
            self.error(text)
        )
    }

    pub fn changed(&self, text: impl Display) -> String {
        format!(
            "{} {}",
            paint(self.enabled, "~", &["1", "33"]),
            self.warning(text)
        )
    }

    pub fn ok(&self, text: impl Display) -> String {
        format!(
            "{} {}",
            paint(self.enabled, "✓", &["1", "32"]),
            self.success(text)
        )
    }

    pub fn fail(&self, text: impl Display) -> String {
        format!(
            "{} {}",
            paint(self.enabled, "x", &["1", "31"]),
            self.error(text)
        )
    }

    pub fn command_line(&self, command: impl Display) -> String {
        format!("{} {}", self.success("$"), self.command(command))
    }

    pub fn rule(&self, width: usize) -> String {
        self.muted("-".repeat(width))
    }
}

pub fn stdout() -> Palette {
    Palette::stdout()
}

fn color_enabled(is_tty: bool) -> bool {
    if !is_tty {
        return false;
    }
    if env::var_os("NO_COLOR").is_some() {
        return false;
    }
    if matches!(env::var("CLICOLOR").as_deref(), Ok("0")) {
        return false;
    }
    true
}

fn paint(enabled: bool, text: impl Display, codes: &[&str]) -> String {
    let text = text.to_string();
    if !enabled || codes.is_empty() {
        return text;
    }
    format!("\x1b[{}m{text}\x1b[0m", codes.join(";"))
}
