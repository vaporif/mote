use std::sync::LazyLock;

use regex::Regex;

/// Starts with a letter or underscore, then letters, digits, or underscores.
static ANNOTATION_KEY_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^[\p{L}_][\p{L}\p{N}_]*$").expect("valid regex"));

pub fn is_valid_annotation_key(key: &str) -> bool {
    !key.is_empty() && ANNOTATION_KEY_RE.is_match(key)
}

pub fn is_reserved_annotation_key(key: &str) -> bool {
    key.starts_with('$') || key.starts_with("0x")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid_annotation_keys() {
        assert!(is_valid_annotation_key("name"));
        assert!(is_valid_annotation_key("_private"));
        assert!(is_valid_annotation_key("key123"));
        assert!(is_valid_annotation_key("über"));
        assert!(is_valid_annotation_key("名前"));
        assert!(is_valid_annotation_key("_"));
    }

    #[test]
    fn invalid_annotation_keys() {
        assert!(!is_valid_annotation_key(""));
        assert!(!is_valid_annotation_key("123abc"));
        assert!(!is_valid_annotation_key("key-name"));
        assert!(!is_valid_annotation_key("key name"));
    }

    #[test]
    fn reserved_prefix_rejected() {
        assert!(is_reserved_annotation_key("$owner"));
        assert!(is_reserved_annotation_key("$key"));
        assert!(is_reserved_annotation_key("0xAddress"));
        assert!(!is_reserved_annotation_key("owner"));
        assert!(!is_reserved_annotation_key("_0x"));
    }
}
