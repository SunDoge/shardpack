pub(crate) fn crc32c(bytes: &[u8]) -> u32 {
    crc32c_fast::crc32c(bytes)
}

pub(crate) fn crc32c_slices(first: &[u8], second: &[u8]) -> u32 {
    crc32c::crc32c_combine(crc32c(first), crc32c(second), second.len())
}

#[cfg(test)]
mod tests {
    use super::crc32c;

    #[test]
    fn matches_castagnoli_check_value() {
        assert_eq!(crc32c(b""), 0);
        assert_eq!(crc32c(b"123456789"), 0xe306_9283);
        assert_eq!(
            super::crc32c_slices(b"1234", b"56789"),
            crc32c(b"123456789")
        );
    }
}
