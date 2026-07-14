use std::{error::Error, sync::Arc};

use tokio::sync::{
    mpsc::{self, Receiver, Sender},
    Notify,
};
use tokio_util::task::AbortOnDropHandle;
use tracing::error;

pub struct SmartBuffer<T>
where
    T: Send + 'static,
{
    _refill_task: AbortOnDropHandle<()>,
    receiver: Receiver<T>,
    start_refill: Arc<Notify>,
}

impl<T> SmartBuffer<T>
where
    T: Send + 'static,
{
    pub fn new<F, Fut, E>(size: usize, refill: F) -> Self
    where
        F: (FnOnce(Sender<T>) -> Fut) + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
        E: Error,
    {
        let (sender, receiver) = mpsc::channel(size);
        let start_refill = Arc::new(Notify::new());

        let start_refill_clone = start_refill.clone();
        let handle = tokio::spawn(async move {
            start_refill_clone.notified().await;
            if let Err(e) = refill(sender).await {
                error!("Refill faied: {}", e);
            };
        });

        let refill_task = AbortOnDropHandle::new(handle);
        SmartBuffer {
            _refill_task: refill_task,
            receiver,
            start_refill,
        }
    }

    pub async fn next(&mut self) -> Option<T> {
        self.start_refill.notify_one();
        self.receiver.recv().await
    }
}

#[cfg(test)]
mod test {
    use std::sync::{atomic::AtomicUsize, Arc};

    use tokio::sync::mpsc::Sender;

    use crate::smart_buffer::SmartBuffer;

    /// This test ensures that SmartBuffer iterates through all elements
    /// provided by refill function.
    #[tokio::test]
    async fn iterate_all_elements() {
        let expected = vec![1, 2, 3, 4];
        let elements = expected.clone();
        let refill_fn = |sender: Sender<i32>| async move {
            for el in elements {
                let _ = sender.send(el).await;
            }
            Result::<(), std::convert::Infallible>::Ok(())
        };
        let mut buffer = SmartBuffer::new(2, refill_fn);

        let mut actual = Vec::new();
        while let Some(v) = buffer.next().await {
            actual.push(v);
        }

        assert_eq!(actual, expected)
    }

    /// This test ensures that SmartBuffer calls refill function lazily.
    #[tokio::test]
    async fn refill_on_demand() {
        let counter = Arc::new(AtomicUsize::new(0));

        let counter_clone = counter.clone();
        let mut buffer = SmartBuffer::new(1, move |sender: Sender<()>| async move {
            counter_clone.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let _ = sender.send(()).await;
            Result::<(), std::convert::Infallible>::Ok(())
        });
        assert_eq!(counter.load(std::sync::atomic::Ordering::SeqCst), 0);

        let _ = buffer.next().await;
        assert_eq!(counter.load(std::sync::atomic::Ordering::SeqCst), 1);
    }
}
