/// This module contains tests of the integration between the REST client and
/// the REST server.

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use engine::Reader;
    use kvs_rest_client::RestKeyValueReader;
    use kvs_rest_server::kvs_reader_router;
    use kvs_tck::{
        mock_reader::MockReader,
        reader_assertions::{
            assert_reads_selected_by_key, assert_reads_selected_by_name,
            assert_reads_selected_by_namespace, assert_reads_selected_by_nnk,
            assert_reads_unlimited, using_mock_reader,
        },
    };
    use tokio::{net::TcpListener, test};
    use tracing_test::traced_test;

    #[test]
    #[traced_test]
    async fn ensure_reads_unlimited_test() {
        using_mock_reader(async |backend| {
            let frontend = init_frontend(backend).await;
            assert_reads_unlimited(frontend).await;
        })
        .await;
    }

    #[test]
    #[traced_test]
    async fn ensure_reads_selected_by_namespace_test() {
        using_mock_reader(async |backend| {
            let frontend = init_frontend(backend).await;
            assert_reads_selected_by_namespace(frontend).await;
        })
        .await;
    }

    #[test]
    #[traced_test]
    async fn ensure_reads_selected_by_name_test() {
        using_mock_reader(async |backend| {
            let frontend = init_frontend(backend).await;
            assert_reads_selected_by_name(frontend).await;
        })
        .await;
    }

    #[test]
    #[traced_test]
    async fn ensure_reads_selected_by_key_test() {
        using_mock_reader(async |backend| {
            let frontend = init_frontend(backend).await;
            assert_reads_selected_by_key(frontend).await;
        })
        .await;
    }

    #[test]
    #[traced_test]
    async fn ensure_reads_selected_by_nnk_test() {
        using_mock_reader(async |backend| {
            let frontend = init_frontend(backend).await;
            assert_reads_selected_by_nnk(frontend).await;
        })
        .await;
    }

    #[test]
    #[traced_test]
    async fn limit_is_unsupported() {
        using_mock_reader(async |backend| {
            let frontend = init_frontend(backend).await;
            let result = frontend.read(&|it| it.limit(&1).select()).await;

            assert!(result.is_err());
        })
        .await;
    }

    async fn init_frontend(backend: MockReader) -> RestKeyValueReader {
        let backend = Arc::new(backend);
        let kvs_reader_app = kvs_reader_router(backend.clone());
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("Failed to bind to random port");
        let addr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            axum::serve(listener, kvs_reader_app)
                .await
                .expect("server failed");
        });

        let page_size = 2;
        let uri = format!("http://{}", addr);
        RestKeyValueReader::new(&uri, page_size)
    }
}
