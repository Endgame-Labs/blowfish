//! A direct reader for the JSON number arrays of a query vector (Round G24).
//!
//! `serde_json` reads each element through its generic byte reader, a `deserialize_any` visitor,
//! and an enum, about nine percent of a warm query's CPU at 1,024 dimensions. [`parse`] reads the
//! array's raw text in one loop instead. It must give the same `f32` as that path for every
//! input, so it follows `serde_json`'s own (non-`float_roundtrip`) number rule exactly: the
//! digits form a `u64` significand, a decimal point and an exponent give a power of ten, and the
//! value is `significand as f64` times or divided by that power of ten, then cast to `f32`. Any
//! number outside the range where that rule is easy to restate (more than 18 significant digits,
//! a power of ten beyond `1e308`, a result that overflows) makes [`parse`] answer `None`, and the
//! caller reads the same text with `serde_json` as before.
use std::sync::OnceLock;
/// TEMPORARY (Round G24 A/B): bit 2 reads vectors the old way.
pub static G24_SWITCH: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);

/// `1e0` through `1e308`, as `serde_json` tabulates them: `str::parse` rounds correctly, as a
/// literal does.
fn pow10() -> &'static [f64; 309] {
    static TABLE: OnceLock<[f64; 309]> = OnceLock::new();
    TABLE.get_or_init(|| std::array::from_fn(|n| format!("1e{n}").parse().unwrap()))
}

/// The elements of `text`, a JSON array of plain numbers, as `f32` (through `f64`, as
/// `decode_vector` reads them). `None` if `text` is anything else or a number needs `serde_json`.
pub fn parse(text: &str) -> Option<Vec<f32>> {
    if G24_SWITCH.load(std::sync::atomic::Ordering::Relaxed) & 2 != 0 {
        return None;
    }
    let bytes = text.as_bytes();
    let mut i = skip(bytes, 0);
    if bytes.get(i) != Some(&b'[') {
        return None;
    }
    i = skip(bytes, i + 1);
    let mut out = Vec::with_capacity(text.len() / 12);
    if bytes.get(i) == Some(&b']') {
        return Some(out);
    }
    loop {
        let (value, next) = number(bytes, i)?;
        out.push(value as f32);
        i = skip(bytes, next);
        match bytes.get(i)? {
            b',' => i = skip(bytes, i + 1),
            b']' => return Some(out),
            _ => return None,
        }
    }
}

fn skip(bytes: &[u8], mut i: usize) -> usize {
    while matches!(bytes.get(i), Some(b' ' | b'\t' | b'\n' | b'\r')) {
        i += 1;
    }
    i
}

/// One JSON number starting at `i`: its value as `serde_json` reads it, and the index after it.
fn number(b: &[u8], mut i: usize) -> Option<(f64, usize)> {
    let negative = b.get(i) == Some(&b'-');
    i += usize::from(negative);
    let (mut significand, mut digits, mut exponent) = (0u64, 0u32, 0i32);
    let mut digit = |c: u8, significand: &mut u64| -> Option<()> {
        // Leading zeros are not significant digits, and the u64 cannot overflow below 19.
        if *significand != 0 || c != b'0' {
            digits += 1;
            if digits > 18 {
                return None;
            }
        }
        *significand = *significand * 10 + u64::from(c - b'0');
        Some(())
    };
    match b.get(i)? {
        b'0' => i += 1,
        c @ b'1'..=b'9' => {
            digit(*c, &mut significand)?;
            i += 1;
            while let Some(c @ b'0'..=b'9') = b.get(i) {
                digit(*c, &mut significand)?;
                i += 1;
            }
        }
        _ => return None,
    }
    let mut decimal = false;
    if b.get(i) == Some(&b'.') {
        decimal = true;
        i += 1;
        let start = i;
        while let Some(c @ b'0'..=b'9') = b.get(i) {
            digit(*c, &mut significand)?;
            exponent -= 1;
            i += 1;
        }
        if i == start {
            return None;
        }
    }
    let mut scientific = false;
    if matches!(b.get(i), Some(b'e' | b'E')) {
        scientific = true;
        i += 1;
        let positive = match b.get(i)? {
            b'+' => {
                i += 1;
                true
            }
            b'-' => {
                i += 1;
                false
            }
            _ => true,
        };
        let start = i;
        let mut e = 0i32;
        while let Some(c @ b'0'..=b'9') = b.get(i) {
            // Beyond three digits the power of ten is out of the table.
            if i - start >= 3 {
                return None;
            }
            e = e * 10 + i32::from(c - b'0');
            i += 1;
        }
        if i == start {
            return None;
        }
        exponent += if positive { e } else { -e };
    }
    let value = if decimal || scientific {
        let mut f = significand as f64;
        let power = *pow10().get(exponent.unsigned_abs() as usize)?;
        if exponent >= 0 {
            f *= power;
            if f.is_infinite() {
                return None;
            }
        } else {
            f /= power;
        }
        f
    } else {
        // An integer: `serde_json` reads it as a `u64` or `i64`, and `-0` as `-0.0`.
        significand as f64
    };
    Some((if negative { -value } else { value }, i))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// What serving did before: `serde_json` reads each number to `f64`, then it is cast.
    fn reference(text: &str) -> Vec<f32> {
        serde_json::from_str::<Vec<f64>>(text)
            .unwrap()
            .into_iter()
            .map(|x| x as f32)
            .collect()
    }
    fn same(text: &str) {
        let fast = parse(text).unwrap_or_else(|| panic!("{text} should take the fast path"));
        let slow = reference(text);
        assert_eq!(fast.len(), slow.len(), "{text}");
        for (a, b) in fast.iter().zip(&slow) {
            assert_eq!(a.to_bits(), b.to_bits(), "{text}: {a:e} against {b:e}");
        }
    }

    #[test]
    fn edge_cases_match_serde_json_bit_for_bit() {
        for text in [
            "[0]",
            "[-0]",
            "[0.0]",
            "[-0.0]",
            "[-0.0e0]",
            "[1e-45]",
            "[1.4e-45]",
            "[1.401298464324817e-45]",
            "[7e-46]",
            "[7.1e-46]",
            "[1e-46]",
            "[1.17549435e-38]",
            "[1.1754942e-38]",
            "[3.4028235e38]",
            "[3.4028236e38]",
            "[3.4028235677973366e38]",
            "[1e38]",
            "[1e-300]",
            "[1E5]",
            "[1e+5]",
            "[1e-5]",
            "[12345678901234567]",
            "[123456789012345678]",
            "[-123456789012345678]",
            "[0.12345678901234567]",
            "[0.010000000707805157]",
            "[-0.03150000050663948]",
            "[0.00000000000000001234]",
            "[1.0000000596046448]",
            "[1.00000002980232239]",
            "[16777217]",
            "[16777217.0]",
            "[0.1, 0.2, -3, 4e-3, 1e10]",
            "[ 1 , 2 ,\n3\t]",
            "[]",
            "[ ]",
            "[1e22, 1e23, 1e-22, 1e-23]",
            "[9007199254740993]",
            "[9007199254740993.0]",
        ] {
            same(text);
        }
    }

    #[test]
    fn what_serde_json_must_read_goes_back_to_it() {
        for text in [
            "[1, \"x\"]",
            "[null]",
            "[[1, 2]]",
            "\"AACAPwAAAAA=\"",
            "5",
            "{\"a\": 1}",
            "[1e309]",
            "[1e-400]",
            "[1e1000]",
            "[12345678901234567890]",
            "[1.2345678901234567890123]",
            "[1e400, 2]",
        ] {
            assert!(parse(text).is_none(), "{text}");
        }
    }

    #[test]
    fn random_f32_texts_match_in_every_form() {
        let mut seed = 0x2545_F491_4F6C_DD1Du64;
        let mut next = move || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        for round in 0..200_000 {
            let bits = next() as u32;
            let x = f32::from_bits(bits);
            if !x.is_finite() {
                continue;
            }
            // The shortest f32 text, the shortest f64 text of the f32 (what clients send), and the
            // f64 of a perturbed value (17 digits, not an f32 at all).
            let perturbed = f64::from(x) * (1. + (next() % 1000) as f64 * 1e-12);
            for text in [
                format!("[{x}]"),
                format!("[{:?}]", f64::from(x)),
                format!("[{perturbed:?}]"),
                format!("[{:e}]", f64::from(x)),
                format!("[{:.9e}]", f64::from(x)),
            ] {
                // Texts `serde_json` must read itself (19 or more digits, extreme powers) are
                // not the fast path's: they only have to agree when it takes them.
                if let Some(fast) = parse(&text) {
                    let slow = reference(&text);
                    assert_eq!(fast[0].to_bits(), slow[0].to_bits(), "{round}: {text}");
                }
            }
        }
    }

    #[test]
    fn query_sized_arrays_take_the_fast_path_and_match() {
        let mut seed = 99u64;
        let mut next = move || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        // 1,024 components like a normalized embedding: f32 values printed as f64 shortest text.
        let text = format!(
            "[{}]",
            (0..1024)
                .map(|_| {
                    let x = ((next() % 20_001) as f32 - 10_000.) / 100_000.;
                    format!("{:?}", f64::from(x))
                })
                .collect::<Vec<_>>()
                .join(",")
        );
        same(&text);
    }
}
