//! SQLite-compatible date/time functions.
//!
//! Supports: date(), time(), datetime(), strftime(), julianday(), unixepoch().
//! Handles ISO 8601 date strings and the special 'now' value.
//! Modifier support: +/- N days/months/years/hours/minutes/seconds,
//! start of month/year/day.

use std::time::SystemTime;

/// Internal date-time representation.
#[derive(Debug, Clone, Copy)]
struct DateTime {
    year: i32,
    month: u32,  // 1..=12
    day: u32,    // 1..=31
    hour: u32,   // 0..=23
    minute: u32, // 0..=59
    second: u32, // 0..=59
}

impl DateTime {
    fn now() -> Self {
        let secs = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;
        Self::from_unix(secs)
    }

    fn from_unix(ts: i64) -> Self {
        // Convert unix timestamp to date-time components (UTC).
        let mut remaining = ts;
        let negative = remaining < 0;
        if negative {
            // Adjust to a positive epoch for calculation.
            // We handle this by shifting forward by a multiple of 400-year cycles.
            let cycles = (-remaining / 12622780800) + 1;
            remaining += cycles * 12622780800;
        }

        let secs_in_day: i64 = 86400;
        let mut days = remaining / secs_in_day;
        let day_secs = (remaining % secs_in_day) as u32;

        let hour = day_secs / 3600;
        let minute = (day_secs % 3600) / 60;
        let second = day_secs % 60;

        // Days since 1970-01-01.
        // Algorithm from Howard Hinnant's date algorithms.
        days += 719468; // shift epoch from 1970-01-01 to 0000-03-01
        let era = if days >= 0 {
            days / 146097
        } else {
            (days - 146096) / 146097
        };
        let doe = (days - era * 146097) as u32; // day of era [0, 146096]
        let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365; // year of era
        let y = (yoe as i64 + era * 400) as i32;
        let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // day of year [0, 365]
        let mp = (5 * doy + 2) / 153; // month [0, 11]
        let d = doy - (153 * mp + 2) / 5 + 1;
        let m = if mp < 10 { mp + 3 } else { mp - 9 };
        let year = if m <= 2 { y + 1 } else { y };

        if negative {
            // Undo the 400-year cycle shifts we added.
            let cycles = ((-ts) / 12622780800) + 1;
            let shift = (cycles * 400) as i32;
            DateTime {
                year: year - shift,
                month: m,
                day: d,
                hour,
                minute,
                second,
            }
        } else {
            DateTime {
                year,
                month: m,
                day: d,
                hour,
                minute,
                second,
            }
        }
    }

    fn to_unix(&self) -> i64 {
        // Convert date-time components to unix timestamp (UTC).
        let y = if self.month <= 2 {
            self.year as i64 - 1
        } else {
            self.year as i64
        };
        let m = if self.month <= 2 {
            self.month as i64 + 9
        } else {
            self.month as i64 - 3
        };
        let era = if y >= 0 {
            y / 400
        } else {
            (y - 399) / 400
        };
        let yoe = (y - era * 400) as u64;
        let doy = (153 * m as u64 + 2) / 5 + self.day as u64 - 1;
        let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
        let days = era * 146097 + doe as i64 - 719468;
        days * 86400 + self.hour as i64 * 3600 + self.minute as i64 * 60 + self.second as i64
    }

    fn to_julian_day(&self) -> f64 {
        // Julian day number: days since -4713-11-24 12:00:00 UTC.
        let unix = self.to_unix() as f64;
        // Unix epoch (1970-01-01 00:00:00) = Julian day 2440587.5
        unix / 86400.0 + 2440587.5
    }

    fn format_date(&self) -> String {
        format!("{:04}-{:02}-{:02}", self.year, self.month, self.day)
    }

    fn format_time(&self) -> String {
        format!("{:02}:{:02}:{:02}", self.hour, self.minute, self.second)
    }

    fn format_datetime(&self) -> String {
        format!(
            "{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
            self.year, self.month, self.day, self.hour, self.minute, self.second
        )
    }

    fn format_strftime(&self, fmt: &str) -> String {
        let mut result = String::with_capacity(fmt.len());
        let mut chars = fmt.chars();
        while let Some(c) = chars.next() {
            if c == '%' {
                match chars.next() {
                    Some('Y') => result.push_str(&format!("{:04}", self.year)),
                    Some('m') => result.push_str(&format!("{:02}", self.month)),
                    Some('d') => result.push_str(&format!("{:02}", self.day)),
                    Some('H') => result.push_str(&format!("{:02}", self.hour)),
                    Some('M') => result.push_str(&format!("{:02}", self.minute)),
                    Some('S') => result.push_str(&format!("{:02}", self.second)),
                    Some('j') => {
                        result.push_str(&format!("{:03}", day_of_year(self.year, self.month, self.day)));
                    }
                    Some('w') => {
                        result.push_str(&format!("{}", day_of_week(self.year, self.month, self.day)));
                    }
                    Some('s') => {
                        result.push_str(&format!("{}", self.to_unix()));
                    }
                    Some('%') => result.push('%'),
                    Some(other) => {
                        result.push('%');
                        result.push(other);
                    }
                    None => result.push('%'),
                }
            } else {
                result.push(c);
            }
        }
        result
    }

    /// Apply a single modifier string to this DateTime, returning a new one.
    fn apply_modifier(&self, modifier: &str) -> Option<DateTime> {
        let m = modifier.trim();
        let lower = m.to_ascii_lowercase();

        if lower == "start of month" {
            return Some(DateTime {
                year: self.year,
                month: self.month,
                day: 1,
                hour: 0,
                minute: 0,
                second: 0,
            });
        }
        if lower == "start of year" {
            return Some(DateTime {
                year: self.year,
                month: 1,
                day: 1,
                hour: 0,
                minute: 0,
                second: 0,
            });
        }
        if lower == "start of day" {
            return Some(DateTime {
                year: self.year,
                month: self.month,
                day: self.day,
                hour: 0,
                minute: 0,
                second: 0,
            });
        }

        // Parse "+N unit" or "-N unit"
        let (sign, rest) = if let Some(r) = m.strip_prefix('+') {
            (1i64, r.trim())
        } else if let Some(r) = m.strip_prefix('-') {
            (-1i64, r.trim())
        } else {
            return None;
        };

        let mut parts = rest.splitn(2, ' ');
        let n_str = parts.next()?;
        let unit = parts.next()?.trim().to_ascii_lowercase();
        let n: i64 = n_str.parse().ok()?;
        let amount = sign * n;

        match unit.trim_end_matches('s').as_ref() {
            "day" => {
                let unix = self.to_unix() + amount * 86400;
                Some(DateTime::from_unix(unix))
            }
            "hour" => {
                let unix = self.to_unix() + amount * 3600;
                Some(DateTime::from_unix(unix))
            }
            "minute" => {
                let unix = self.to_unix() + amount * 60;
                Some(DateTime::from_unix(unix))
            }
            "second" => {
                let unix = self.to_unix() + amount;
                Some(DateTime::from_unix(unix))
            }
            "month" => {
                let total_months = self.year as i64 * 12 + (self.month as i64 - 1) + amount;
                let new_year = if total_months >= 0 {
                    (total_months / 12) as i32
                } else {
                    ((total_months + 1) / 12 - 1) as i32
                };
                let new_month = ((total_months % 12 + 12) % 12 + 1) as u32;
                let max_day = days_in_month(new_year, new_month);
                let new_day = self.day.min(max_day);
                Some(DateTime {
                    year: new_year,
                    month: new_month,
                    day: new_day,
                    hour: self.hour,
                    minute: self.minute,
                    second: self.second,
                })
            }
            "year" => {
                let new_year = self.year + amount as i32;
                let max_day = days_in_month(new_year, self.month);
                let new_day = self.day.min(max_day);
                Some(DateTime {
                    year: new_year,
                    month: self.month,
                    day: new_day,
                    hour: self.hour,
                    minute: self.minute,
                    second: self.second,
                })
            }
            _ => None,
        }
    }
}

fn is_leap_year(y: i32) -> bool {
    (y % 4 == 0 && y % 100 != 0) || (y % 400 == 0)
}

fn days_in_month(year: i32, month: u32) -> u32 {
    match month {
        1 => 31,
        2 => {
            if is_leap_year(year) {
                29
            } else {
                28
            }
        }
        3 => 31,
        4 => 30,
        5 => 31,
        6 => 30,
        7 => 31,
        8 => 31,
        9 => 30,
        10 => 31,
        11 => 30,
        12 => 31,
        _ => 30,
    }
}

fn day_of_year(year: i32, month: u32, day: u32) -> u32 {
    let mut doy = 0;
    for m in 1..month {
        doy += days_in_month(year, m);
    }
    doy + day
}

fn day_of_week(year: i32, month: u32, day: u32) -> u32 {
    // Zeller-like: 0 = Sunday, 1 = Monday, ..., 6 = Saturday (SQLite convention).
    // Use the unix timestamp approach: 1970-01-01 is Thursday (4).
    let dt = DateTime {
        year,
        month,
        day,
        hour: 12,
        minute: 0,
        second: 0,
    };
    let unix = dt.to_unix();
    let days = unix / 86400;
    // 1970-01-01 is Thursday = day 4
    (((days % 7 + 4) % 7 + 7) % 7) as u32
}

/// Parse a time-value string into a DateTime. Supported formats:
/// - 'now'
/// - 'YYYY-MM-DD'
/// - 'YYYY-MM-DD HH:MM:SS'
/// - 'YYYY-MM-DD HH:MM'
/// - 'HH:MM:SS' (time only, date defaults to 2000-01-01)
/// - 'HH:MM' (time only, date defaults to 2000-01-01)
/// - integer unix timestamp
fn parse_timevalue(s: &str) -> Option<DateTime> {
    let s = s.trim();
    if s.eq_ignore_ascii_case("now") {
        return Some(DateTime::now());
    }

    // Try unix timestamp (plain integer).
    if let Ok(ts) = s.parse::<i64>() {
        return Some(DateTime::from_unix(ts));
    }

    // 'YYYY-MM-DD HH:MM:SS' or 'YYYY-MM-DD HH:MM'
    if s.len() >= 16 && s.as_bytes().get(4) == Some(&b'-') && s.as_bytes().get(10) == Some(&b' ') {
        let date_part = &s[..10];
        let time_part = &s[11..];
        let dt = parse_date(date_part)?;
        let (h, m, sec) = parse_time_str(time_part)?;
        return Some(DateTime {
            year: dt.year,
            month: dt.month,
            day: dt.day,
            hour: h,
            minute: m,
            second: sec,
        });
    }

    // Try 'YYYY-MM-DDTHH:MM:SS' (ISO 8601 with T separator).
    if s.len() >= 16 && s.as_bytes().get(4) == Some(&b'-') && s.as_bytes().get(10) == Some(&b'T') {
        let date_part = &s[..10];
        let time_part = &s[11..];
        let dt = parse_date(date_part)?;
        let (h, m, sec) = parse_time_str(time_part)?;
        return Some(DateTime {
            year: dt.year,
            month: dt.month,
            day: dt.day,
            hour: h,
            minute: m,
            second: sec,
        });
    }

    // 'YYYY-MM-DD'
    if s.len() == 10 && s.as_bytes().get(4) == Some(&b'-') {
        return parse_date(s);
    }

    // 'HH:MM:SS' or 'HH:MM'
    if s.len() >= 5 && s.as_bytes().get(2) == Some(&b':') {
        let (h, m, sec) = parse_time_str(s)?;
        return Some(DateTime {
            year: 2000,
            month: 1,
            day: 1,
            hour: h,
            minute: m,
            second: sec,
        });
    }

    None
}

fn parse_date(s: &str) -> Option<DateTime> {
    if s.len() != 10 {
        return None;
    }
    let year: i32 = s[..4].parse().ok()?;
    if s.as_bytes()[4] != b'-' {
        return None;
    }
    let month: u32 = s[5..7].parse().ok()?;
    if s.as_bytes()[7] != b'-' {
        return None;
    }
    let day: u32 = s[8..10].parse().ok()?;
    if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return None;
    }
    Some(DateTime {
        year,
        month,
        day,
        hour: 0,
        minute: 0,
        second: 0,
    })
}

fn parse_time_str(s: &str) -> Option<(u32, u32, u32)> {
    // HH:MM:SS or HH:MM
    let parts: Vec<&str> = s.split(':').collect();
    if parts.len() < 2 {
        return None;
    }
    let h: u32 = parts[0].parse().ok()?;
    let m: u32 = parts[1].parse().ok()?;
    let sec: u32 = if parts.len() >= 3 {
        // Handle fractional seconds by truncating.
        let sec_str = parts[2].split('.').next().unwrap_or("0");
        sec_str.parse().ok()?
    } else {
        0
    };
    if h > 23 || m > 59 || sec > 59 {
        return None;
    }
    Some((h, m, sec))
}

/// Apply a sequence of modifier strings to a DateTime.
fn apply_modifiers(mut dt: DateTime, modifiers: &[String]) -> Option<DateTime> {
    for modifier in modifiers {
        dt = dt.apply_modifier(modifier)?;
    }
    Some(dt)
}

// ---------------------------------------------------------------------------
// Public API: functions callable from both engines
// ---------------------------------------------------------------------------

/// Evaluate SQLite `date(timevalue, modifier, ...)` — returns 'YYYY-MM-DD'.
pub fn eval_date(args: &[String]) -> Option<String> {
    if args.is_empty() {
        return Some(DateTime::now().format_date());
    }
    let dt = parse_timevalue(&args[0])?;
    let dt = apply_modifiers(dt, &args[1..])?;
    Some(dt.format_date())
}

/// Evaluate SQLite `time(timevalue, modifier, ...)` — returns 'HH:MM:SS'.
pub fn eval_time(args: &[String]) -> Option<String> {
    if args.is_empty() {
        return Some(DateTime::now().format_time());
    }
    let dt = parse_timevalue(&args[0])?;
    let dt = apply_modifiers(dt, &args[1..])?;
    Some(dt.format_time())
}

/// Evaluate SQLite `datetime(timevalue, modifier, ...)` — returns 'YYYY-MM-DD HH:MM:SS'.
pub fn eval_datetime(args: &[String]) -> Option<String> {
    if args.is_empty() {
        return Some(DateTime::now().format_datetime());
    }
    let dt = parse_timevalue(&args[0])?;
    let dt = apply_modifiers(dt, &args[1..])?;
    Some(dt.format_datetime())
}

/// Evaluate SQLite `strftime(format, timevalue, modifier, ...)` — returns formatted string.
pub fn eval_strftime(args: &[String]) -> Option<String> {
    if args.len() < 2 {
        return None;
    }
    let fmt = &args[0];
    let dt = parse_timevalue(&args[1])?;
    let dt = apply_modifiers(dt, &args[2..])?;
    Some(dt.format_strftime(fmt))
}

/// Evaluate SQLite `julianday(timevalue)` — returns Julian day number as f64.
pub fn eval_julianday(args: &[String]) -> Option<f64> {
    if args.is_empty() {
        return Some(DateTime::now().to_julian_day());
    }
    let dt = parse_timevalue(&args[0])?;
    let dt = apply_modifiers(dt, &args[1..])?;
    Some(dt.to_julian_day())
}

/// Evaluate SQLite `unixepoch(timevalue)` — returns Unix timestamp as i64.
pub fn eval_unixepoch(args: &[String]) -> Option<i64> {
    if args.is_empty() {
        return Some(DateTime::now().to_unix());
    }
    let dt = parse_timevalue(&args[0])?;
    let dt = apply_modifiers(dt, &args[1..])?;
    Some(dt.to_unix())
}

/// Returns `true` if `name` (lowercased) is one of the date/time scalar functions.
pub fn is_datetime_fn(name: &str) -> bool {
    matches!(
        name,
        "date" | "time" | "datetime" | "strftime" | "julianday" | "unixepoch"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(v: &str) -> String {
        v.to_string()
    }

    #[test]
    fn date_basic() {
        assert_eq!(eval_date(&[s("2024-01-15")]), Some(s("2024-01-15")));
    }

    #[test]
    fn date_with_modifier_plus_month() {
        assert_eq!(
            eval_date(&[s("2024-01-15"), s("+1 month")]),
            Some(s("2024-02-15"))
        );
    }

    #[test]
    fn date_with_modifier_minus_days() {
        assert_eq!(
            eval_date(&[s("2024-01-15"), s("-10 days")]),
            Some(s("2024-01-05"))
        );
    }

    #[test]
    fn date_month_clamp() {
        // Jan 31 + 1 month => Feb 29 (2024 is leap year)
        assert_eq!(
            eval_date(&[s("2024-01-31"), s("+1 month")]),
            Some(s("2024-02-29"))
        );
        // Jan 31 + 1 month in non-leap year => Feb 28
        assert_eq!(
            eval_date(&[s("2023-01-31"), s("+1 month")]),
            Some(s("2023-02-28"))
        );
    }

    #[test]
    fn time_basic() {
        assert_eq!(eval_time(&[s("12:30:45")]), Some(s("12:30:45")));
    }

    #[test]
    fn datetime_basic() {
        assert_eq!(
            eval_datetime(&[s("2024-01-15 12:30:45")]),
            Some(s("2024-01-15 12:30:45"))
        );
    }

    #[test]
    fn datetime_plus_hour() {
        assert_eq!(
            eval_datetime(&[s("2024-01-15 12:00:00"), s("+1 hour")]),
            Some(s("2024-01-15 13:00:00"))
        );
    }

    #[test]
    fn datetime_plus_year() {
        assert_eq!(
            eval_datetime(&[s("2024-01-15 00:00:00"), s("+1 year")]),
            Some(s("2025-01-15 00:00:00"))
        );
    }

    #[test]
    fn strftime_year() {
        assert_eq!(
            eval_strftime(&[s("%Y"), s("2024-06-15")]),
            Some(s("2024"))
        );
    }

    #[test]
    fn strftime_full_date() {
        assert_eq!(
            eval_strftime(&[s("%Y-%m-%d"), s("2024-01-15")]),
            Some(s("2024-01-15"))
        );
    }

    #[test]
    fn strftime_day_of_year() {
        // Jan 15 is the 15th day of the year
        assert_eq!(
            eval_strftime(&[s("%j"), s("2024-01-15")]),
            Some(s("015"))
        );
    }

    #[test]
    fn strftime_day_of_week() {
        // 2024-01-15 is a Monday = 1
        assert_eq!(
            eval_strftime(&[s("%w"), s("2024-01-15")]),
            Some(s("1"))
        );
    }

    #[test]
    fn strftime_unix_timestamp() {
        assert_eq!(
            eval_strftime(&[s("%s"), s("1970-01-01 00:00:00")]),
            Some(s("0"))
        );
    }

    #[test]
    fn julianday_known() {
        let jd = eval_julianday(&[s("2024-01-15")]).unwrap();
        // Known Julian day for 2024-01-15 00:00:00 UTC is 2460324.5
        assert!((jd - 2460324.5).abs() < 0.001);
    }

    #[test]
    fn unixepoch_epoch() {
        assert_eq!(
            eval_unixepoch(&[s("1970-01-01 00:00:00")]),
            Some(0)
        );
    }

    #[test]
    fn unixepoch_known() {
        // 2024-01-15 00:00:00 UTC
        let ts = eval_unixepoch(&[s("2024-01-15 00:00:00")]).unwrap();
        assert_eq!(ts, 1705276800);
    }

    #[test]
    fn start_of_month() {
        assert_eq!(
            eval_date(&[s("2024-06-15"), s("start of month")]),
            Some(s("2024-06-01"))
        );
    }

    #[test]
    fn start_of_year() {
        assert_eq!(
            eval_date(&[s("2024-06-15"), s("start of year")]),
            Some(s("2024-01-01"))
        );
    }

    #[test]
    fn start_of_day() {
        assert_eq!(
            eval_datetime(&[s("2024-06-15 14:30:00"), s("start of day")]),
            Some(s("2024-06-15 00:00:00"))
        );
    }

    #[test]
    fn chained_modifiers() {
        assert_eq!(
            eval_date(&[s("2024-01-15"), s("+1 month"), s("+5 days")]),
            Some(s("2024-02-20"))
        );
    }

    #[test]
    fn date_from_datetime_string() {
        assert_eq!(
            eval_date(&[s("2024-01-15 14:30:00")]),
            Some(s("2024-01-15"))
        );
    }

    #[test]
    fn time_from_datetime_string() {
        assert_eq!(
            eval_time(&[s("2024-01-15 14:30:00")]),
            Some(s("14:30:00"))
        );
    }

    #[test]
    fn now_returns_something() {
        // Just check that 'now' doesn't panic and returns a date-like string.
        let result = eval_date(&[s("now")]);
        assert!(result.is_some());
        let d = result.unwrap();
        assert_eq!(d.len(), 10); // YYYY-MM-DD
        assert_eq!(d.as_bytes()[4], b'-');
    }

    #[test]
    fn is_datetime_fn_check() {
        assert!(is_datetime_fn("date"));
        assert!(is_datetime_fn("time"));
        assert!(is_datetime_fn("datetime"));
        assert!(is_datetime_fn("strftime"));
        assert!(is_datetime_fn("julianday"));
        assert!(is_datetime_fn("unixepoch"));
        assert!(!is_datetime_fn("lower"));
        assert!(!is_datetime_fn("count"));
    }
}
