const RECORD_DOMAIN: &[u8] = b"shardpack-record-v1";
const SECONDARY_DOMAIN: &[u8] = b"shardpack-fingerprint-b-v1";

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ContentFingerprint {
    pub record_count: u64,
    pub accumulator_a: [u8; 32],
    pub accumulator_b: [u8; 32],
}

impl ContentFingerprint {
    pub fn insert(&mut self, sample_id: &[u8; 16], flags: u32, payload: &[u8]) {
        let mut record = blake3::Hasher::new();
        record.update(RECORD_DOMAIN);
        record.update(sample_id);
        record.update(&flags.to_le_bytes());
        record.update(payload);
        let digest = *record.finalize().as_bytes();

        let mut secondary = blake3::Hasher::new();
        secondary.update(SECONDARY_DOMAIN);
        secondary.update(&digest);
        let digest_b = *secondary.finalize().as_bytes();

        add_256(&mut self.accumulator_a, &digest);
        add_256(&mut self.accumulator_b, &digest_b);
        self.record_count = self
            .record_count
            .checked_add(1)
            .expect("record count overflow");
    }

    pub fn merge(&mut self, other: &Self) {
        add_256(&mut self.accumulator_a, &other.accumulator_a);
        add_256(&mut self.accumulator_b, &other.accumulator_b);
        self.record_count = self
            .record_count
            .checked_add(other.record_count)
            .expect("record count overflow");
    }
}

fn add_256(target: &mut [u8; 32], value: &[u8; 32]) {
    let mut carry = 0_u16;
    for (left, right) in target.iter_mut().zip(value) {
        let sum = u16::from(*left) + u16::from(*right) + carry;
        *left = sum as u8;
        carry = sum >> 8;
    }
}

#[cfg(test)]
mod tests {
    use super::ContentFingerprint;

    #[test]
    fn order_independent_and_multiplicity_sensitive() {
        let records = [
            ([1; 16], b"alpha".as_slice()),
            ([2; 16], b"beta".as_slice()),
        ];
        let mut forward = ContentFingerprint::default();
        let mut reverse = ContentFingerprint::default();
        for (id, payload) in records {
            forward.insert(&id, 0, payload);
        }
        for (id, payload) in records.into_iter().rev() {
            reverse.insert(&id, 0, payload);
        }
        assert_eq!(forward, reverse);
        reverse.insert(&records[0].0, 0, records[0].1);
        assert_ne!(forward, reverse);
    }

    #[test]
    fn record_flags_are_part_of_logical_content() {
        let mut left = ContentFingerprint::default();
        let mut right = ContentFingerprint::default();
        left.insert(&[1; 16], 1, b"payload");
        right.insert(&[1; 16], 2, b"payload");
        assert_ne!(left, right);
    }
}
