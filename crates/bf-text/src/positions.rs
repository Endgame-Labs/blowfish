//! Lossless per-posting position deltas. A scalar block omits element ordinals.
use super::*;

fn put_varint(out: &mut Vec<u8>, mut value: u32) {
    while value >= 128 {
        out.push(value as u8 | 128);
        value >>= 7;
    }
    out.push(value as u8);
}

fn varint(c: &mut Cursor<'_>) -> io::Result<u32> {
    let mut value = 0;
    for shift in (0..35).step_by(7) {
        let byte = c.take(1)?[0];
        if shift == 28 && byte > 15 {
            return Err(invalid("overflowing position varint"));
        }
        value |= u32::from(byte & 127) << shift;
        if byte < 128 {
            if shift > 0 && byte == 0 {
                return Err(invalid("noncanonical position varint"));
            }
            return Ok(value);
        }
    }
    Err(invalid("unterminated position varint"))
}

pub(super) fn encode<'a>(
    documents: impl Iterator<Item = &'a [(u32, u32)]> + Clone,
    out: &mut Vec<u8>,
) {
    let arrays = documents.clone().any(|d| d.iter().any(|&(e, _)| e != 0));
    out.push(u8::from(arrays));
    for positions in documents {
        let (mut element, mut position) = (0, 0);
        for &(e, p) in positions {
            if arrays {
                put_varint(out, e - element);
            }
            if e != element {
                position = 0;
            }
            put_varint(out, p - position);
            (element, position) = (e, p);
        }
    }
}

pub(super) struct Decoder<'a> {
    c: Cursor<'a>,
    compressed: bool,
    arrays: bool,
}
impl<'a> Decoder<'a> {
    pub(super) fn new(bytes: &'a [u8], compressed: bool) -> io::Result<Self> {
        let mut c = Cursor::new(bytes);
        let arrays = if compressed {
            match c.take(1)?[0] {
                0 => false,
                1 => true,
                _ => return Err(invalid("invalid position block flags")),
            }
        } else {
            true
        };
        Ok(Self {
            c,
            compressed,
            arrays,
        })
    }

    pub(super) fn document(&mut self, tf: u32) -> io::Result<Vec<(u32, u32)>> {
        // Every occurrence consumes at least one byte, two for array blocks,
        // or eight for legacy blocks. Bound allocation before trusting TF.
        let minimum = if !self.compressed {
            8
        } else if self.arrays {
            2
        } else {
            1
        };
        if u64::from(tf) * minimum > (self.c.data.len() - self.c.at) as u64 {
            return Err(invalid("truncated positions"));
        }
        let mut out = Vec::with_capacity(tf as usize);
        let (mut element, mut position) = (0u32, 0u32);
        for _ in 0..tf {
            if self.compressed {
                let delta = if self.arrays { varint(&mut self.c)? } else { 0 };
                element = element
                    .checked_add(delta)
                    .ok_or_else(|| invalid("position element overflow"))?;
                if delta != 0 {
                    position = 0;
                }
                position = position
                    .checked_add(varint(&mut self.c)?)
                    .ok_or_else(|| invalid("position overflow"))?;
                out.push((element, position));
            } else {
                out.push((self.c.u32()?, self.c.u32()?));
            }
        }
        if !out.windows(2).all(|p| p[0] <= p[1]) {
            return Err(invalid("unsorted positions"));
        }
        Ok(out)
    }

    pub(super) fn finish(self) -> io::Result<()> {
        if self.c.at != self.c.data.len() {
            return Err(invalid("trailing positions"));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn arbitrary_sorted_scalar_and_array_positions_roundtrip(
            documents in prop::collection::vec(prop::collection::vec((any::<u32>(), any::<u32>()), 0..128), 0..8),
            scalar in any::<bool>(),
        ) {
            let documents: Vec<_> = documents.into_iter().map(|mut positions| {
                if scalar { for p in &mut positions { p.0 = 0; } }
                positions.sort_unstable();
                positions
            }).collect();
            let mut bytes = Vec::new();
            encode(documents.iter().map(Vec::as_slice), &mut bytes);
            let mut decoder = Decoder::new(&bytes, true).unwrap();
            for positions in &documents {
                prop_assert_eq!(decoder.document(positions.len() as u32).unwrap(), positions.clone());
            }
            prop_assert!(decoder.finish().is_ok());
        }
    }

    #[test]
    fn scalar_omits_elements_and_resets_position_per_document() {
        let documents = [vec![(0, 0), (0, 127), (0, 128)], vec![(0, 3)]];
        let mut bytes = Vec::new();
        encode(documents.iter().map(Vec::as_slice), &mut bytes);
        assert_eq!(bytes, [0, 0, 127, 1, 3]);
        let mut decoder = Decoder::new(&bytes, true).unwrap();
        assert_eq!(decoder.document(3).unwrap(), documents[0]);
        assert_eq!(decoder.document(1).unwrap(), documents[1]);
        decoder.finish().unwrap();
    }

    #[test]
    fn decoder_rejects_overflow_truncation_flags_and_noncanonical_values() {
        assert!(Decoder::new(&[], true).is_err());
        assert!(Decoder::new(&[2], true).is_err());
        for bytes in [
            vec![0, 128],
            vec![0, 128, 0],
            vec![0, 255, 255, 255, 255, 16],
            vec![0, 255, 255, 255, 255, 143, 1],
        ] {
            assert!(Decoder::new(&bytes, true).unwrap().document(1).is_err());
        }
        for bytes in [
            vec![0, 255, 255, 255, 255, 15, 1],       // position addition
            vec![1, 255, 255, 255, 255, 15, 0, 1, 0], // element addition
        ] {
            assert!(Decoder::new(&bytes, true).unwrap().document(2).is_err());
        }
        assert!(
            Decoder::new(&[0, 0], true)
                .unwrap()
                .document(u32::MAX)
                .is_err()
        );
        assert!(Decoder::new(&[0, 0], true).unwrap().finish().is_err());
        assert!(Decoder::new(&[0; 8], false).unwrap().document(2).is_err());
    }
}
