use bytes::Bytes;

/// Retain only the most recent allocation, never a growing free-list. Each
/// reader has one stored-body slot and one decoded-payload slot. Jumbo blocks
/// are not retained. External records/slices always take precedence over reuse.
#[derive(Default)]
pub(crate) struct ReusableBuffer {
    previous: Option<Bytes>,
}

const MAX_RETAINED_CAPACITY: usize = 64 * 1024 * 1024;

impl ReusableBuffer {
    pub(crate) fn take(&mut self, required: usize) -> Vec<u8> {
        if let Some(bytes) = self.previous.take()
            && bytes.is_unique()
        {
            // Only full buffers created by retain() enter this slot. Conversion
            // takes ownership of the allocation; no record may still reference
            // it when is_unique is true.
            let mut output = Vec::from(bytes);
            if output.capacity() < required {
                output.reserve_exact(required - output.len());
            }
            return output;
        }
        Vec::with_capacity(required)
    }

    pub(crate) fn retain(&mut self, buffer: Vec<u8>) -> Bytes {
        let retain = buffer.capacity() <= MAX_RETAINED_CAPACITY;
        let bytes = Bytes::from(buffer);
        self.previous = retain.then(|| bytes.clone());
        bytes
    }

    pub(crate) fn clear(&mut self) {
        self.previous = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reuse_waits_for_every_slice_and_preserves_full_allocation() {
        let mut slot = ReusableBuffer::default();
        let bytes = slot.retain(vec![7; 4096]);
        let pointer = bytes.as_ptr();
        let slice = bytes.slice(100..200);
        drop(bytes);
        let mut other = slot.take(4096);
        other.resize(4096, 9);
        assert_ne!(other.as_ptr(), pointer);
        assert_eq!(slice.as_ref(), &[7; 100]);
        drop(slice);
        let bytes = slot.retain(other);
        let pointer = bytes.as_ptr();
        drop(bytes);
        let reused = slot.take(4096);
        assert_eq!(reused.as_ptr(), pointer);
        assert_eq!(reused.len(), 4096);
    }

    #[test]
    fn varying_lengths_keep_capacity_without_exposing_previous_bytes() {
        let mut slot = ReusableBuffer::default();
        for (index, size) in [8192, 1, 16384, 0, 37].into_iter().enumerate() {
            let mut buffer = slot.take(size);
            assert!(buffer.capacity() >= size);
            buffer.resize(size, 0);
            buffer.fill(index as u8);
            let bytes = slot.retain(buffer);
            assert_eq!(bytes.len(), size);
            assert!(bytes.iter().all(|value| *value == index as u8));
        }
    }

    #[test]
    fn jumbo_allocations_are_not_retained_and_clear_releases_ownership() {
        let mut slot = ReusableBuffer::default();
        // Capacity, rather than length, determines retention.
        let mut jumbo = Vec::with_capacity(MAX_RETAINED_CAPACITY + 1);
        jumbo.push(1);
        let bytes = slot.retain(jumbo);
        assert!(slot.previous.is_none());
        drop(bytes);
        let bytes = slot.retain(vec![2; 10]);
        assert!(!bytes.is_unique());
        slot.clear();
        assert!(bytes.is_unique());
    }
}
