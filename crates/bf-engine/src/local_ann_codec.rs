//! Lossless local-cache packing of f16 words. Normalized embeddings have a zero high
//! exponent bit; omitting it saves one bit per component. Exceptional blocks stay raw,
//! including NaNs, infinities, large values, odd tails, and arbitrary non-vector bytes.
use std::io;

/// Raw bytes per independently checksummed block.
pub(super) const BLOCK: usize = 16 << 10;

/// Encode a block, retaining the original bytes when any high exponent bit is set.
pub(super) fn encode(raw: &[u8]) -> Vec<u8> {
    if !raw.len().is_multiple_of(16) || raw.as_chunks::<2>().0.iter().any(|x| x[1] & 0x40 != 0) {
        return raw.to_vec();
    }
    let mut out = Vec::with_capacity(raw.len() / 16 * 15);
    for group in raw.as_chunks::<16>().0 {
        let mut packed = 0u128;
        for (j, word) in group.as_chunks::<2>().0.iter().enumerate() {
            let x = u16::from_le_bytes([word[0], word[1]]);
            let x = (x & 0x3fff) | ((x >> 1) & 0x4000);
            packed |= u128::from(x) << (j * 15);
        }
        out.extend_from_slice(&packed.to_le_bytes()[..15]);
    }
    out
}

/// Reconstruct a block. The caller checks its encoded CRC before calling this function.
pub(super) fn decode(encoded: &[u8], raw_len: usize) -> io::Result<Vec<u8>> {
    if encoded.len() == raw_len {
        return Ok(encoded.to_vec());
    }
    if !raw_len.is_multiple_of(16) || encoded.len() != raw_len / 16 * 15 {
        return Err(io::Error::other("invalid local exponent block"));
    }
    let mut out = vec![0; raw_len];
    for (group, target) in encoded
        .as_chunks::<15>()
        .0
        .iter()
        .zip(out.as_chunks_mut::<16>().0.iter_mut())
    {
        let lo = u64::from_le_bytes(group[..8].try_into().expect("eight bytes"));
        let hi = u64::from_le_bytes(group[7..].try_into().expect("eight bytes"));
        for j in 0..4 {
            let x = (lo >> (j * 15)) as u16;
            target[j * 2..j * 2 + 2]
                .copy_from_slice(&((x & 0x3fff) | ((x & 0x4000) << 1)).to_le_bytes());
        }
        for j in 4..8 {
            let x = (hi >> (j * 15 - 56)) as u16;
            target[j * 2..j * 2 + 2]
                .copy_from_slice(&((x & 0x3fff) | ((x & 0x4000) << 1)).to_le_bytes());
        }
    }
    Ok(out)
}

/// Validate the cumulative physical block ends before trusting completion metadata.
pub(super) fn validate(ends: &[u32], len: u64) -> io::Result<()> {
    if ends.len() as u64 != len.div_ceil(BLOCK as u64) {
        return Err(io::Error::other("invalid local exponent index"));
    }
    let mut start = 0u32;
    for (i, &end) in ends.iter().enumerate() {
        let raw = (len - i as u64 * BLOCK as u64).min(BLOCK as u64);
        let stored = end.checked_sub(start).map(u64::from);
        if stored != Some(raw) && !(raw.is_multiple_of(16) && stored == Some(raw / 16 * 15)) {
            return Err(io::Error::other("invalid local exponent extent"));
        }
        start = end;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_f16_pattern_round_trips_including_signed_zero_and_nan_payloads() {
        for high in 0..=u16::MAX / 8 {
            let raw: Vec<_> = (0..8u16)
                .flat_map(|lo| (high * 8 + lo).to_le_bytes())
                .collect();
            let encoded = encode(&raw);
            assert_eq!(decode(&encoded, raw.len()).unwrap(), raw);
            assert_eq!(encoded.len() == 15, (high * 8) & 0x4000 == 0);
        }
    }

    #[test]
    fn partial_blocks_and_invalid_indexes() {
        for len in 0..=BLOCK + 1 {
            let raw = vec![0; len];
            let encoded = encode(&raw);
            assert_eq!(decode(&encoded, len).unwrap(), raw);
        }
        assert!(decode(&[0; 14], 16).is_err());
        assert!(validate(&[15360, 31744], 32768).is_ok());
        for ends in [vec![], vec![0], vec![1], vec![15360, 15359]] {
            assert!(validate(&ends, BLOCK as u64).is_err());
        }
    }
}
