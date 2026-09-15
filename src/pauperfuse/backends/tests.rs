use super::fixture::{FixtureError, VersionedProducer};
use super::*;

async fn source_from(producer: &VersionedProducer) -> Source {
    let mut observed = producer.observe().await.unwrap();
    let entry = observed.next_entry().await.unwrap().unwrap();
    let Description::File { source, .. } = entry.description else {
        panic!("expected a file");
    };
    source
}

#[tokio::test]
async fn observe_is_ordered_metadata_without_opening_bytes() {
    let mut producer = VersionedProducer::new();
    producer.publish("z", "last", "one", b"not read");
    producer.publish("a", "first", "one", b"also not read");
    let mut tree = producer.observe().await.unwrap();
    let expected = ["a", "z"]
        .into_iter()
        .zip(["first", "last"])
        .map(|(path, output)| TreeEntry {
            path: RelPath::try_new(vec![path.into()]).unwrap(),
            description: Description::File {
                source: Source {
                    backend: producer.id.clone(),
                    output: OutputVersion {
                        output: output.as_bytes().to_vec(),
                        version: b"one".to_vec(),
                    },
                },
                size: None,
            },
        })
        .collect::<Vec<_>>();
    let mut actual = Vec::new();
    while let Some(entry) = tree.next_entry().await.unwrap() {
        actual.push(entry);
    }
    assert_eq!(actual, expected);
    assert_eq!(tree.next_entry().await.unwrap(), None);
    assert_eq!(
        producer
            .activity
            .opens
            .load(std::sync::atomic::Ordering::Relaxed),
        0
    );
    assert_eq!(
        producer
            .activity
            .reads
            .load(std::sync::atomic::Ordering::Relaxed),
        0
    );
}

#[tokio::test]
async fn byte_reader_fills_only_the_requested_range_and_reports_eof() {
    let mut producer = VersionedProducer::new();
    producer.publish("a", "note", "one", b"abcdefgh");
    let source = source_from(&producer).await;
    let mut reader = ProducerAccess::new(&producer).open(&source).await.unwrap();
    let mut buffer = [0xcc; 4];
    assert_eq!(reader.read_at(2, &mut buffer).await.unwrap(), 4);
    assert_eq!(&buffer, b"cdef");
    buffer.fill(0xcc);
    assert_eq!(reader.read_at(6, &mut buffer).await.unwrap(), 2);
    assert_eq!(buffer, [b'g', b'h', 0xcc, 0xcc]);
    for offset in [8, 9, u64::MAX] {
        buffer.fill(0xcc);
        assert_eq!(reader.read_at(offset, &mut buffer).await.unwrap(), 0);
        assert_eq!(buffer, [0xcc; 4]);
    }
    assert_eq!(reader.read_at(0, &mut []).await.unwrap(), 0);
    assert_eq!(
        producer
            .activity
            .opens
            .load(std::sync::atomic::Ordering::Relaxed),
        1
    );
    assert_eq!(
        producer
            .activity
            .reads
            .load(std::sync::atomic::Ordering::Relaxed),
        6
    );
}

#[tokio::test]
async fn previously_observed_source_opens_its_retained_version_not_latest() {
    let mut producer = VersionedProducer::new();
    producer.publish("a", "note", "one", b"old");
    let old_source = source_from(&producer).await;
    producer.publish("a", "note", "two", b"new");
    let new_source = source_from(&producer).await;
    assert_ne!(old_source, new_source);
    let mut old = ProducerAccess::new(&producer)
        .open(&old_source)
        .await
        .unwrap();
    let mut latest = ProducerAccess::new(&producer)
        .open(&new_source)
        .await
        .unwrap();
    let mut buffer = [0; 3];
    assert_eq!(old.read_at(0, &mut buffer).await.unwrap(), 3);
    assert_eq!(&buffer, b"old");
    assert_eq!(latest.read_at(0, &mut buffer).await.unwrap(), 3);
    assert_eq!(&buffer, b"new");
}

#[tokio::test]
async fn evicted_version_errors_while_an_open_reader_retains_its_bytes() {
    let mut producer = VersionedProducer::new();
    producer.publish("a", "note", "one", b"old");
    let old_source = source_from(&producer).await;
    let mut opened = ProducerAccess::new(&producer)
        .open(&old_source)
        .await
        .unwrap();
    producer.publish("a", "note", "two", b"new");
    producer.evict(&old_source.output);
    let result = ProducerAccess::new(&producer).open(&old_source).await;
    match result {
        Err(AccessError::Producer(error)) => {
            assert_eq!(error, FixtureError::Unavailable(old_source.output));
        }
        Err(AccessError::WrongBackend { .. }) => panic!("source has the right backend"),
        Ok(_) => panic!("evicted version must not open latest"),
    }
    let mut buffer = [0; 3];
    assert_eq!(opened.read_at(0, &mut buffer).await.unwrap(), 3);
    assert_eq!(&buffer, b"old");
}

#[tokio::test]
async fn producer_access_refuses_wrong_backend_without_opening_output() {
    let mut producer = VersionedProducer::new();
    producer.publish("a", "note", "one", b"old");
    let mut source = source_from(&producer).await;
    source.backend = BackendId("another-backend".into());
    let result = ProducerAccess::new(&producer).open(&source).await;
    match result {
        Err(AccessError::WrongBackend {
            requested,
            available,
        }) => {
            assert_eq!(requested, source.backend);
            assert_eq!(available, producer.id);
        }
        Err(AccessError::Producer(_)) => panic!("wrong backend must not reach producer"),
        Ok(_) => panic!("wrong backend must be rejected"),
    }
    assert_eq!(
        producer
            .activity
            .opens
            .load(std::sync::atomic::Ordering::Relaxed),
        0
    );
    assert_eq!(
        producer
            .activity
            .reads
            .load(std::sync::atomic::Ordering::Relaxed),
        0
    );
}
