#[cfg(test)]
mod tests {
    use axum::Router;
    use kvs_api::KeyValueStorageReader;
    use kvs_rest_client::RestKeyValueReader;
    use kvs_rest_server::kvs_reader_router;
    use kvs_tck::ensure_reader_compatibility;
    use std::sync::Arc;
    use tokio::net::TcpListener;
    use tracing_test::traced_test;

    #[tokio::test]
    #[traced_test]
    async fn ensure_reader_client_compatibility() {
        ensure_reader_compatibility(|original_reader| async {
            let listener = TcpListener::bind("127.0.0.1:0")
                .await
                .expect("failed to bind to random port");
            let addr = listener.local_addr().unwrap();
            let app = Router::new().merge(kvs_reader_router(original_reader));

            tokio::spawn(async move {
                axum::serve(listener, app).await.expect("server failed");
            });

            let client = RestKeyValueReader::new(&format!("http://{}", addr));
            Arc::new(client) as KeyValueStorageReader
        })
        .await;
    }
}
