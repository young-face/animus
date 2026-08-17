#[cfg(test)]
mod tests {
    use std::{net::SocketAddr, sync::Arc};

    use axum::Router;
    use engine::Reader;
    use futures::StreamExt;
    use kvs_api::KeyValueRow;
    use kvs_rest_client::RestKeyValueReader;
    use kvs_rest_server::kvs_reader_router;
    use kvs_tck::reader_utils::MockReader;
    use tokio::net::TcpListener;
    use tracing_test::traced_test;

    #[tokio::test]
    #[traced_test]
    async fn read_all_when_unlimited() {
        let expected = vec![
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
        ];
        let original_reader = Arc::new(MockReader::new(&expected));
        let addr = serve(kvs_reader_router(original_reader)).await;
        let reader = RestKeyValueReader::new(&format!("http://{}", addr), 1);

        let (stream, _) = reader
            .read(&|it| it.select())
            .await
            .expect("Reading failed");

        let actual: Vec<_> = stream.collect().await;

        assert_eq!(expected, actual)
    }

    #[tokio::test]
    #[traced_test]
    async fn read_limited() {
        let data = vec![
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
        ];
        let original_reader = Arc::new(MockReader::new(&data));
        let addr = serve(kvs_reader_router(original_reader)).await;
        let reader = RestKeyValueReader::new(&format!("http://{}", addr), 1);

        let (stream, _) = reader
            .read(&|it| it.limit(&1).select())
            .await
            .expect("Reading failed");

        let expected = vec![Ok(KeyValueRow::new(
            "robots",
            "T-1000",
            "classification",
            "Infiltration and Assasination Unit",
        ))];
        let actual: Vec<_> = stream.collect().await;

        assert_eq!(expected, actual)
    }

    async fn serve(app: Router) -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("Failed to bind to random port");
        let addr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            axum::serve(listener, app).await.expect("server failed");
        });

        addr
    }
}
