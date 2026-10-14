//! The unambiguous entry-key layout shared by tiers that address several
//! independent sets inside one group.
//!
//! A set segment is `d` for the default set, or `n<len>:<name>` for a named
//! one, where `<len>` is the name's length in bytes, in decimal. The
//! discriminator makes the default set a distinct address from every name
//! (the empty name included), and the length prefix means the name never has
//! to be scanned for a separator — so a name or a key may contain any
//! character, `/` and `:` included, without two `(set, key)` pairs ever
//! sharing an entry. A reader that knows the base can split any key back into
//! its set and tail; that is what makes the layout injective.

use std::fmt::Write as _;

/// Appends the set segment for `set` (`None`: the default set) to `out`.
pub(crate) fn push_set(out: &mut String, set: Option<&str>) {
    match set {
        None => out.push('d'),
        Some(name) => {
            // Writing into a `String` cannot fail.
            let _ = write!(out, "n{}:", name.len());
            out.push_str(name);
        }
    }
}

/// `<base>/<set>` — a per-set entry with no further key.
pub(crate) fn set_key(base: &str, set: Option<&str>) -> String {
    let mut out = String::with_capacity(base.len() + 2 + set.map_or(0, |n| n.len() + 6));
    out.push_str(base);
    out.push('/');
    push_set(&mut out, set);
    out
}

/// The prefix `<base>/<set>/` that per-key entries of one set share.
pub(crate) fn set_prefix(base: &str, set: Option<&str>) -> String {
    let mut out = set_key(base, set);
    out.push('/');
    out
}

#[cfg(test)]
mod tests {
    use super::{set_key, set_prefix};

    /// Splits a `<base>/<set>/<tail>` key back into its parts — the inverse
    /// the layout guarantees, used here to prove it is injective.
    fn parse<'k>(base: &str, key: &'k str) -> Option<(Option<&'k str>, &'k str)> {
        let rest = key.strip_prefix(base)?.strip_prefix('/')?;
        let (set, rest) = if let Some(rest) = rest.strip_prefix('d') {
            (None, rest)
        } else {
            let rest = rest.strip_prefix('n')?;
            let (len, rest) = rest.split_once(':')?;
            let len: usize = len.parse().ok()?;
            let name = rest.get(..len)?;
            (Some(name), &rest[len..])
        };
        Some((set, rest.strip_prefix('/')?))
    }

    #[test]
    fn the_default_set_and_every_name_are_distinct_addresses() {
        assert_eq!(set_key("~x", None), "~x/d");
        assert_eq!(set_key("~x", Some("")), "~x/n0:");
        assert_eq!(set_key("~x", Some("lsn")), "~x/n3:lsn");
        assert_eq!(set_key("~x", Some("d")), "~x/n1:d");
        assert_ne!(set_key("~x", None), set_key("~x", Some("")));
        assert_ne!(set_key("~x", None), set_key("~x", Some("d")));
    }

    #[test]
    fn separators_inside_names_and_keys_cannot_collide() {
        let pairs: [(Option<&str>, &str); 8] = [
            (None, "lsn:s7"),
            (Some("lsn"), "s7"),
            (None, "n3:lsn/s7"),
            (Some("lsn/s7"), ""),
            (Some("a/b"), "c"),
            (Some("a"), "b/c"),
            (Some(""), "x"),
            (None, "x"),
        ];
        let keys: Vec<String> = pairs
            .iter()
            .map(|(set, tail)| format!("{}{tail}", set_prefix("~floor", *set)))
            .collect();
        for (i, key) in keys.iter().enumerate() {
            assert_eq!(parse("~floor", key), Some(pairs[i]), "{key}");
            for other in &keys[i + 1..] {
                assert_ne!(key, other);
            }
        }
    }
}
