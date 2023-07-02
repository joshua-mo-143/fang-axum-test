use axum::{routing::get, Router};
use fang::{AsyncQueue, NoTls};
use sqlx::PgPool;
use tokio::time::{sleep, Duration};
use fang::asynk::{async_queue::AsyncQueueable, async_worker_pool::AsyncWorkerPool};
use fang::serde::{Deserialize, Serialize};
use fang::{async_trait, AsyncRunnable, Scheduled};

use tracing::info;

struct CustomService {
    router: Router,
    queue: AsyncQueue<NoTls>,
    pool: PgPool,
}

async fn hello_world() -> &'static str {
    "Hello, world!"
}

#[shuttle_runtime::main]
async fn axum(
    #[shuttle_shared_db::Postgres] pool: PgPool,
    #[shuttle_fang::Postgres] queue: AsyncQueue<NoTls>,
) -> Result<CustomService, shuttle_runtime::Error> {
    sqlx::migrate!().run(&pool).await;
    
    let router = Router::new().route("/", get(hello_world));

    Ok(CustomService { router, queue, pool })
}

#[shuttle_runtime::async_trait]
impl shuttle_runtime::Service for CustomService {
    async fn bind(mut self, addr: std::net::SocketAddr) -> Result<(), shuttle_runtime::Error> {
        let server = axum::Server::bind(&addr).serve(self.router.into_make_service());

        let meme = do_task(self.queue);

        tokio::select! {
            _ = server => {},
            _ = meme => {}

        }

        Ok(())
    }
}

async fn do_task(mut queue: AsyncQueue<NoTls>) -> Result<(), shuttle_runtime::Error> {
    
        let mut wpool: AsyncWorkerPool<AsyncQueue<NoTls>> = AsyncWorkerPool::builder()
            .number_of_workers(1u32)
            .queue(queue.clone())
            // if you want to run tasks of the specific kind
            .task_type("my-task-type")
            .build();
            wpool.start().await;
    
    loop {
    queue.insert_task(&AsyncTask{number: 3u16} as &dyn AsyncRunnable ).await.unwrap();
    sleep(Duration::from_secs(5)).await;

    }
    Ok(())
}

#[derive(Serialize, Deserialize)]
#[serde(crate = "fang::serde")]
struct AsyncTask {
    pub number: u16,
}

#[typetag::serde]
#[async_trait]
impl AsyncRunnable for AsyncTask {
    async fn run(&self, _queueable: &mut dyn AsyncQueueable) -> Result<(), fang::FangError> {
        info!("Hello world!");
        Ok(())
    }
    // this func is optional
    // Default task_type is common
    fn task_type(&self) -> String {
        "my-task-type".to_string()
    }

    // This will be useful if you would like to schedule tasks.
    // default value is None (the task is not scheduled, it's just executed as soon as it's inserted)
    fn cron(&self) -> Option<Scheduled> {
        None
    }

    // the maximum number of retries. Set it to 0 to make it not retriable
    // the default value is 20
    fn max_retries(&self) -> i32 {
        20
    }

    // backoff mode for retries
    fn backoff(&self, attempt: u32) -> u32 {
        u32::pow(2, attempt)
    }
}
