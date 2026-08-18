/// This module contains tests of integration between the REST client and the
/// REST server. It focuses on the integration aspects ensuring the data
/// transfers correctly.

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use engine::Reader;
    use futures::StreamExt;
    use kvs_api::{KeyValueRow, KeyValueStorageReaderError};
    use kvs_rest_client::RestKeyValueReader;
    use kvs_rest_server::kvs_reader_router;
    use kvs_tck::reader_utils::MockReader;
    use tokio::net::TcpListener;
    use tracing_test::traced_test;

    #[tokio::test]
    #[traced_test]
    async fn read_all() {
        let reader = setup_client(1).await;

        let (stream, _) = reader
            .read(&|it| it.select())
            .await
            .expect("Reading failed");

        let actual: Vec<_> = stream.collect().await;
        let expexted = test_data();

        assert_eq!(expexted, actual)
    }

    #[tokio::test]
    #[traced_test]
    async fn read_to_the_limit() {
        let reader = setup_client(1).await;

        let (stream, _) = reader
            .read(&|it| it.limit(&1).select())
            .await
            .expect("Reading failed");

        let first_row = test_data()[0].clone();
        let expected = vec![first_row];
        let actual: Vec<_> = stream.collect().await;

        assert_eq!(expected, actual)
    }

    fn test_data() -> Vec<Result<KeyValueRow, KeyValueStorageReaderError>> {
        vec![
            Ok(KeyValueRow::new(
                "robots",
                "T-1000",
                "classification",
                "Infiltration and Assasination Unit",
            )),
            Ok(KeyValueRow::new(
                "robots",
                "T-1000",
                "estimated_mass",
                "140",
            )),
        ]
    }

    async fn setup_client(page_size: usize) -> RestKeyValueReader {
        let test_data = test_data();
        let original_reader = Arc::new(MockReader::new(&test_data));
        let app = kvs_reader_router(original_reader);
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("Failed to bind to random port");
        let addr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            axum::serve(listener, app).await.expect("server failed");
        });

        RestKeyValueReader::new(&format!("http://{}", addr), page_size)
    }
}
