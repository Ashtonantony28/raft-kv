const POLY: u32 = 0x82F6_3B78;

const TABLE: [u32; 256] = {
    let mut table = [0u32; 256];
    let mut i = 0;
    while i < 256 {
        let mut c = i as u32;
        let mut k = 0;
        while k < 8 {
            c = if c & 1 != 0 { (c >> 1) ^ POLY } else { c >> 1 };
            k += 1;
        }
        table[i] = c;
        i += 1;
    }
    table
};

pub fn crc32c(parts: &[&[u8]]) -> u32 {
    let mut crc = !0u32;
    for &b in parts.iter().flat_map(|p| p.iter()) {
        crc = TABLE[((crc ^ b as u32) & 0xFF) as usize] ^ (crc >> 8);
    }
    !crc
}

#[cfg(test)]
mod tests {
    use super::crc32c;

    #[test]
    fn known_vectors() {
        assert_eq!(crc32c(&[b""]), 0);
        assert_eq!(crc32c(&[b"123456789"]), 0xE306_9283);
        assert_eq!(crc32c(&[b"1234", b"56789"]), 0xE306_9283);
        assert_eq!(crc32c(&[&[0u8; 32]]), 0x8A91_36AA);
    }
}
