use futures::Future;
use std::time::Duration;

pub async fn delay_for(duration: Duration) {
    tokio::time::delay_for(duration).await
}

pub async fn timeout<T, O>(duration: Duration, future: T) -> Option<O>
where
    T: Future<Output = O>,
{
    tokio::time::timeout(duration, future).await.ok()
}
