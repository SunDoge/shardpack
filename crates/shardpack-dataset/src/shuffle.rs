use async_trait::async_trait;

use crate::{AsyncSourceReader, EncodedRecord, Error, OfflineShuffleOptions, Result, SourceReader};

struct ShuffleBuffer {
    options: OfflineShuffleOptions,
    records: Vec<EncodedRecord>,
    bytes: usize,
    rng: XorShift64,
}

impl ShuffleBuffer {
    fn new(options: OfflineShuffleOptions) -> Result<Self> {
        if options.max_records == 0 || options.max_bytes == 0 {
            return Err(Error::InvalidOption {
                field: "offline_shuffle",
                reason: "record and byte limits must be greater than zero",
            });
        }
        Ok(Self {
            options,
            records: Vec::new(),
            bytes: 0,
            rng: XorShift64(options.seed.max(1)),
        })
    }

    fn can_accept(&self, record: &EncodedRecord) -> bool {
        self.records.len() < self.options.max_records
            && self
                .bytes
                .checked_add(record.payload.len())
                .is_some_and(|bytes| bytes <= self.options.max_bytes)
    }

    fn push(&mut self, record: EncodedRecord) {
        self.bytes = self
            .bytes
            .checked_add(record.payload.len())
            .expect("record was accepted by checked byte bound");
        self.records.push(record);
    }

    fn pop(&mut self) -> Option<EncodedRecord> {
        if self.records.is_empty() {
            return None;
        }
        let index = self.rng.index(self.records.len());
        let record = self.records.swap_remove(index);
        self.bytes -= record.payload.len();
        Some(record)
    }
}

pub(crate) struct ShuffledSource<'a, S> {
    inner: &'a mut S,
    buffer: Option<ShuffleBuffer>,
    pending: Option<EncodedRecord>,
    exhausted: bool,
}

impl<'a, S> ShuffledSource<'a, S> {
    pub(crate) fn new(inner: &'a mut S, options: Option<OfflineShuffleOptions>) -> Result<Self> {
        Ok(Self {
            inner,
            buffer: options.map(ShuffleBuffer::new).transpose()?,
            pending: None,
            exhausted: false,
        })
    }
}

impl<S: SourceReader> SourceReader for ShuffledSource<'_, S> {
    fn next_record(&mut self) -> Result<Option<EncodedRecord>> {
        let Some(buffer) = &mut self.buffer else {
            return self.inner.next_record();
        };
        loop {
            let next = if let Some(record) = self.pending.take() {
                Some(record)
            } else if self.exhausted {
                return Ok(buffer.pop());
            } else {
                match self.inner.next_record()? {
                    Some(record) => Some(record),
                    None => {
                        self.exhausted = true;
                        return Ok(buffer.pop());
                    }
                }
            };
            let record = next.expect("record is present");
            if record.payload.len() > buffer.options.max_bytes {
                if buffer.records.is_empty() {
                    return Ok(Some(record));
                }
                self.pending = Some(record);
                return Ok(buffer.pop());
            }
            if buffer.can_accept(&record) {
                buffer.push(record);
            } else {
                self.pending = Some(record);
                return Ok(buffer.pop());
            }
        }
    }
}

pub(crate) struct AsyncShuffledSource<'a, S> {
    inner: &'a mut S,
    buffer: Option<ShuffleBuffer>,
    pending: Option<EncodedRecord>,
    exhausted: bool,
}

impl<'a, S> AsyncShuffledSource<'a, S> {
    pub(crate) fn new(inner: &'a mut S, options: Option<OfflineShuffleOptions>) -> Result<Self> {
        Ok(Self {
            inner,
            buffer: options.map(ShuffleBuffer::new).transpose()?,
            pending: None,
            exhausted: false,
        })
    }
}

#[async_trait]
impl<S: AsyncSourceReader + Send> AsyncSourceReader for AsyncShuffledSource<'_, S> {
    async fn next_record(&mut self) -> Result<Option<EncodedRecord>> {
        let Some(buffer) = &mut self.buffer else {
            return self.inner.next_record().await;
        };
        loop {
            let next = if let Some(record) = self.pending.take() {
                Some(record)
            } else if self.exhausted {
                return Ok(buffer.pop());
            } else {
                match self.inner.next_record().await? {
                    Some(record) => Some(record),
                    None => {
                        self.exhausted = true;
                        return Ok(buffer.pop());
                    }
                }
            };
            let record = next.expect("record is present");
            if record.payload.len() > buffer.options.max_bytes {
                if buffer.records.is_empty() {
                    return Ok(Some(record));
                }
                self.pending = Some(record);
                return Ok(buffer.pop());
            }
            if buffer.can_accept(&record) {
                buffer.push(record);
            } else {
                self.pending = Some(record);
                return Ok(buffer.pop());
            }
        }
    }
}

struct XorShift64(u64);

impl XorShift64 {
    fn index(&mut self, upper: usize) -> usize {
        let mut value = self.0;
        value ^= value << 13;
        value ^= value >> 7;
        value ^= value << 17;
        self.0 = value;
        (value as usize) % upper
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::ShuffledSource;
    use crate::{EncodedRecord, OfflineShuffleOptions, Result, SourceReader};

    struct Records(std::vec::IntoIter<EncodedRecord>);
    impl SourceReader for Records {
        fn next_record(&mut self) -> Result<Option<EncodedRecord>> {
            Ok(self.0.next())
        }
    }

    fn shuffled(seed: u64) -> Vec<u8> {
        let records = (0..20_u8)
            .map(|value| EncodedRecord {
                sample_id: [value; 16],
                flags: 0,
                payload: Bytes::from(vec![value]),
            })
            .collect::<Vec<_>>();
        let mut source = Records(records.into_iter());
        let mut shuffled = ShuffledSource::new(
            &mut source,
            Some(OfflineShuffleOptions {
                seed,
                max_records: 20,
                max_bytes: 20,
            }),
        )
        .unwrap();
        let mut output = Vec::new();
        while let Some(record) = shuffled.next_record().unwrap() {
            output.push(record.sample_id[0]);
        }
        output
    }

    #[test]
    fn shuffle_is_deterministic_and_preserves_every_record() {
        let first = shuffled(7);
        assert_eq!(first, shuffled(7));
        assert_ne!(first, shuffled(8));
        let mut sorted = first;
        sorted.sort_unstable();
        assert_eq!(sorted, (0..20_u8).collect::<Vec<_>>());
    }

    #[test]
    fn jumbo_record_bypasses_the_bounded_pool_without_splitting() {
        let records = vec![
            EncodedRecord {
                sample_id: [1; 16],
                flags: 0,
                payload: Bytes::from_static(b"a"),
            },
            EncodedRecord {
                sample_id: [2; 16],
                flags: 0,
                payload: Bytes::from(vec![2; 100]),
            },
        ];
        let mut source = Records(records.into_iter());
        let mut shuffled = ShuffledSource::new(
            &mut source,
            Some(OfflineShuffleOptions {
                seed: 3,
                max_records: 2,
                max_bytes: 4,
            }),
        )
        .unwrap();
        let first = shuffled.next_record().unwrap().unwrap();
        let second = shuffled.next_record().unwrap().unwrap();
        assert_eq!(first.payload.len() + second.payload.len(), 101);
        assert!(shuffled.next_record().unwrap().is_none());
    }
}
