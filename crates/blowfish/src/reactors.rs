//! Independent HTTP reactors with round-robin connection ownership.
use axum::{Router, serve::Listener};
use std::{future::Future, io, net::SocketAddr};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{mpsc, watch},
};

struct Connections {
    incoming: mpsc::Receiver<(std::net::TcpStream, SocketAddr)>,
    address: SocketAddr,
}
impl Listener for Connections {
    type Io = TcpStream;
    type Addr = SocketAddr;

    async fn accept(&mut self) -> (TcpStream, SocketAddr) {
        loop {
            let Some((socket, peer)) = self.incoming.recv().await else {
                return std::future::pending().await;
            };
            match TcpStream::from_std(socket) {
                Ok(socket) => return (socket, peer),
                Err(error) => tracing::warn!(%error, "registering HTTP connection"),
            }
        }
    }
    fn local_addr(&self) -> io::Result<SocketAddr> {
        Ok(self.address)
    }
}

/// Serve each connection on one independent single-thread runtime. Warm searches receive
/// their first poll on that reactor; yielded searches continue on the caller's I/O runtime.
/// Shutdown stops acceptance and drains every reactor's existing connections.
pub async fn serve(
    listener: TcpListener,
    app: Router,
    shutdown: impl Future<Output = ()> + Send,
) -> io::Result<()> {
    let count = std::thread::available_parallelism().map_or(1, usize::from);
    serve_count(listener, app, shutdown, count).await
}

async fn serve_count(
    listener: TcpListener,
    app: Router,
    shutdown: impl Future<Output = ()> + Send,
    count: usize,
) -> io::Result<()> {
    let address = listener.local_addr()?;
    let io_runtime = tokio::runtime::Handle::current();
    let (stop, stopped) = watch::channel(false);
    let mut senders = Vec::with_capacity(count);
    let mut completions = tokio::task::JoinSet::new();
    for core in 0..count {
        let (sender, incoming) = mpsc::channel(128);
        senders.push(sender);
        let (app, mut stopped, io_runtime) = (app.clone(), stopped.clone(), io_runtime.clone());
        let (done, finished) = tokio::sync::oneshot::channel();
        std::thread::Builder::new()
            .name(format!("bf-http-{core}"))
            .spawn(move || {
                let result = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .and_then(|runtime| {
                        runtime.block_on(bf_engine::on_query_reactor(io_runtime, async move {
                            axum::serve(Connections { incoming, address }, app)
                                .with_graceful_shutdown(async move {
                                    let _ = stopped.wait_for(|value| *value).await;
                                })
                                .await
                        }))
                    });
                let _ = done.send(result);
            })?;
        completions.spawn(async move {
            finished
                .await
                .unwrap_or_else(|_| Err(io::Error::other("HTTP reactor stopped")))
        });
    }
    tokio::pin!(shutdown);
    let mut next = 0;
    let accepted = loop {
        tokio::select! {
            () = &mut shutdown => break Ok(()),
            finished = completions.join_next() => {
                break finished.unwrap().map_err(io::Error::other).and_then(|result| result);
            }
            connection = listener.accept() => {
                match connection {
                    Ok((socket, peer)) => {
                        let socket = socket.into_std()?;
                        if senders[next].send((socket, peer)).await.is_err() {
                            break Err(io::Error::other("HTTP reactor stopped accepting"));
                        }
                        next = (next + 1) % count;
                    }
                    Err(error) => {
                        tracing::warn!(%error, "accepting HTTP connection");
                        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                    }
                }
            }
        }
    };
    let _ = stop.send(true);
    drop(senders);
    while let Some(result) = completions.join_next().await {
        result.map_err(io::Error::other)??;
    }
    accepted
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::routing::get;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn connections_have_distinct_reactors_and_shutdown_drains() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let app = Router::new().route(
            "/",
            get(|| async {
                assert_eq!(
                    tokio::runtime::Handle::current().runtime_flavor(),
                    tokio::runtime::RuntimeFlavor::CurrentThread
                );
                std::thread::current().name().unwrap().to_owned()
            }),
        );
        let (stop, stopped) = tokio::sync::oneshot::channel();
        let serving = tokio::spawn(serve_count(
            listener,
            app,
            async {
                let _ = stopped.await;
            },
            2,
        ));
        let clients = [reqwest::Client::new(), reqwest::Client::new()];
        let mut names = Vec::new();
        for client in &clients {
            names.push(
                client
                    .get(format!("http://{address}/"))
                    .send()
                    .await
                    .unwrap()
                    .text()
                    .await
                    .unwrap(),
            );
        }
        assert_eq!(names, ["bf-http-0", "bf-http-1"]);
        for (client, expected) in clients.iter().zip(names) {
            assert_eq!(
                client
                    .get(format!("http://{address}/"))
                    .send()
                    .await
                    .unwrap()
                    .text()
                    .await
                    .unwrap(),
                expected
            );
        }
        stop.send(()).unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5), serving)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }
}
