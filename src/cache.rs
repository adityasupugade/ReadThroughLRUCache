use std::collections::HashMap;
use std::sync::atomic::{Ordering, AtomicUsize};
use tokio::runtime::Runtime;
use tokio::sync::mpsc::{Sender, Receiver};
use tokio::sync::oneshot::Sender as oSender;
use std::sync::{RwLock, Arc};
use std::hash::Hash;
use lru::LruCache;
use once_cell::sync::OnceCell;
use tokio::sync::broadcast::{Sender as bSender, Receiver as bReceiver};
use crate::error::{Error, CacheError};
use async_trait::async_trait;
use std::fmt::Debug;

#[derive(Debug, Default)]
pub struct CacheStats {
    pub cache_hit_count          : AtomicUsize,
    pub cache_miss_count         : AtomicUsize,
    pub cache_inflight_hit_count : AtomicUsize,
}

impl Clone for CacheStats {
    fn clone(&self) -> Self {
        CacheStats {
            cache_hit_count          : AtomicUsize::new(self.cache_hit_count.load(Ordering::SeqCst)),
            cache_miss_count         : AtomicUsize::new(self.cache_miss_count.load(Ordering::SeqCst)),
            cache_inflight_hit_count : AtomicUsize::new(self.cache_inflight_hit_count.load(Ordering::SeqCst)),
        }
    }
}

#[derive(Clone)]
pub struct ReadThroughCache<K, V, S> {
    tx          : OnceCell<Sender<CacheAction::<K, V>>>,
    limit       : usize,
    fetch_obj   : Arc<dyn FetchData<K, V, S>>,
}

pub enum CacheAction<K, V> {
    Get(K, oSender<GetResponse<V>>),
    Put(K, Result<V, Error>),
    Stats(oSender<CacheStats>),
    Exit()
}
pub enum GetResponse<V> {
    Data(Result<V, Error>),
    Receiver(bReceiver<Result<V, Error>>),
    DownloadData
}

#[async_trait]
pub trait FetchData<K, V, S>: Send + Sync {
    async fn fetch_data(&self, key: K, fetch_info: S) -> Result<V, Error>;
}

impl <K, V, S> ReadThroughCache<K, V, S>
where K: 'static+PartialEq+Hash+Eq+Send+Clone+Debug, V: 'static+PartialEq+Eq+Send+Clone {
    pub fn new(limit: usize, fetch_obj: Arc<dyn FetchData<K, V, S>>) -> Self {
        Self {tx : OnceCell::new(), limit, fetch_obj}
    }

    pub async fn start(&self, rt: Arc<Runtime>) {
        let (tx, rx) = tokio::sync::mpsc::channel(10);
        let _ = self.tx.set(tx);
        let limit = self.limit;
        rt.spawn(async move { let _ = ReadThroughCache::<K, V, S>::cache_loop(rx, limit).await; });
    }

    async fn cache_loop(mut rx : Receiver<CacheAction<K, V>>, limit: usize) {
        let mut cache: LruCache<K, V> = LruCache::new(limit);
        let mut inflight_dict: HashMap<K, bSender<Result<V, Error>>> = HashMap::new();
        let stats: CacheStats = CacheStats::default();
        loop {
            let action = rx.recv().await;
            if action.is_none() {
                break;
            }
            match action.unwrap() {
                CacheAction::<K, V>::Get(key, tx) => {
                    match cache.get(&key).clone() {
                        Some(data) => {
                            let resp = GetResponse::Data(Ok(data.clone()));
                            let _ = tx.send(resp).map_err(|_err|{
                                let msg = format!("Unable to send data to the caller from cache loop.");
                                tracing::error!(cache_op="cache_loop_get", msg=format!("{:?}", msg).as_str());
                                let err = Error::new(CacheError::TokioSenderError(msg));
                                err
                            });
                            stats.cache_hit_count.fetch_add(1, Ordering::Relaxed);
                            tracing::debug!(cache_op="cache_loop_get", key=format!("{:?}", key).as_str(), "LRU cache hit.");
                            continue;
                        },
                        None => {
                            tracing::debug!(cache_op="cache_loop_get", key=format!("{:?}", key).as_str(), "LRU cache miss.");
                        }
                    }
                    match inflight_dict.get(&key) {
                        Some(broadcast_tx) => {
                            let broadcast_rx = broadcast_tx.subscribe();
                            let _ = tx.send(GetResponse::Receiver(broadcast_rx)).map_err(|_err|{
                                let msg = format!("Unable to send receiver channel to the caller from cache loop.");
                                tracing::error!(cache_op="cache_loop", msg=format!("{:?}", msg).as_str());
                                let err = Error::new(CacheError::TokioSenderError(msg));
                                err
                            });
                            stats.cache_inflight_hit_count.fetch_add(1, Ordering::Relaxed);
                            tracing::debug!(cache_op="cache_loop_get", key=format!("{:?}", key).as_str(), "LRU cache miss, read_through cache hit(inflight)");
                        },
                        None => {
                            let (broadcast_tx, _broadcast_rx) = tokio::sync::broadcast::channel(1);
                            inflight_dict.insert(key.clone(), broadcast_tx);
                            let _ = tx.send(GetResponse::DownloadData).map_err(|_err|{
                                let msg = format!("Unable to send result to the caller from cache loop.");
                                tracing::error!(cache_op="cache_loop", msg=format!("{:?}", msg).as_str());
                                let err = Error::new(CacheError::TokioSenderError(msg));
                                err
                            });
                            stats.cache_miss_count.fetch_add(1, Ordering::Relaxed);
                            tracing::debug!(cache_op="cache_loop_get", key=format!("{:?}", key).as_str(), "LRU cache miss, read_through cache miss.");
                        }
                    }
                }
                CacheAction::Put(key, value) => {
                    if value.is_ok() {
                        let data = value.clone().unwrap();
                        cache.put(key.clone(), data);
                    }
                    let res = inflight_dict.get(&key);
                    match res {
                        Some(broadcast_tx) => {
                            let _ = broadcast_tx.send(value);
                            let _ = inflight_dict.remove_entry(&key);
                        },
                        None => {
                            tracing::error!(cache_op="cache_loop_put", key=format!("{:?}", key).as_str(), "No broadcast channel found.");
                        }
                    } 
                }
                CacheAction::Stats(tx) => {
                    let _ = tx.send(stats.clone()).map_err(|_err|{
                        let msg = format!("Unable to send stats to the caller from cache loop.");
                        tracing::error!(cache_op="cache_loop_stats", msg=format!("{:?}", msg).as_str());
                        let err = Error::new(CacheError::TokioSenderError(msg));
                        err
                    });
                }
                CacheAction::Exit() => {
                    break;
                }
            }
        }
    }

    pub async fn get(&self, key: K, fetch_info: S) -> Result<V, Error> {
        let (data_tx, data_rx) = tokio::sync::oneshot::channel();
        let action = CacheAction::<K, V>::Get(key.clone(), data_tx);
        let _ = self.submit_request(action).await?;
        let resp = data_rx.await.unwrap();
        match resp {
            GetResponse::Data(res) => {
                res
            }
            GetResponse::Receiver(mut rx) => {
                let res = rx.recv().await;
                if res.is_err() {
                    let msg = format!("Received error on receiver channel.");
                    tracing::error!(cache_op="cache_get", msg=format!("{:?}", msg).as_str());
                    return Err(Error::new(CacheError::BroadcastRecvError(msg)));
                }
                res.unwrap()
            }
            GetResponse::DownloadData => {
                let downloaded_data = self.fetch_obj.fetch_data(key.clone(), fetch_info).await;
                tracing::debug!(cache_op="cache_get", key=format!("{:?}", key).as_str(), "Value downloaded");
                self.put(key, downloaded_data.clone()).await?;
                downloaded_data
            }
        }
    }

    async fn put(&self, key: K, value: Result<V, Error>) -> Result<(), Error> {
        let action = CacheAction::<K, V>::Put(key, value.clone());
        self.submit_request(action).await?;
        Ok(())
    }

    pub async fn exit(&self) -> Result<(), Error> {
        let action = CacheAction::<K, V>::Exit();
        let _ = self.submit_request(action).await?;
        Ok(())
    }

    pub async fn stats(&self) -> Result<CacheStats, Error> {
        let (stats_tx, stats_rx) = tokio::sync::oneshot::channel();
        let action = CacheAction::<K, V>::Stats(stats_tx);
        let _ = self.submit_request(action).await?;
        let stats = stats_rx.await.unwrap();
        Ok(stats)
    }

    async fn submit_request(&self, action: CacheAction<K, V>) -> Result<(), Error> {
        let tx = self.tx.get();
        if tx.is_none() {
            return Err(Error::new(CacheError::Unknown("No sender channel found to send data to cache loop.".to_string())));
        }
        let _ = tx.unwrap().send(action).await.map_err(|err|{
            let msg = format!("Unable to send CacheAction to cache loop. Send Error Reason:{}", err);
            tracing::error!(cache_op="cache_submit_request", msg=format!("{:?}", msg).as_str());
            let err = Error::new(crate::error::CacheError::TokioSenderError(msg));
            err
        })?;
        Ok(())
    }
}

#[derive(Clone)]
pub struct BoundedCache<K, V> {
    limit:  usize,
    cache:  Arc<RwLock<HashMap<K, V>>>,
}

impl <K, V> BoundedCache<K, V>
where K: PartialEq+Eq+Hash+Clone, V:Clone {
    pub fn new(limit: usize) -> Self {
        let map = HashMap::new();
        let cache = Arc::new(RwLock::new(map));
        Self {limit, cache}
    }

    pub fn insert(&self, key: K, value: V) -> Result<(), bool> {
        let mut map = self.cache.write().unwrap();
        if map.len() >= self.limit {
            tracing::error!("attempt to store items beyond limit in cache");
            Err(false)
        } else {
            map.insert(key, value);
            Ok(())
        }
    }

    pub fn get(&self, key: &K) -> Option<V> {
        let map = self.cache.read().unwrap();
        map.get(key).cloned()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;
    use std::time::Duration;
    use std::{sync::Arc, collections::HashMap};
    use async_trait::async_trait;
    use tokio::runtime::Runtime;
    use tokio::task::JoinHandle;
    use tokio::time::sleep;
    use crate::error::{Error, CacheError};
    use crate::cache::BoundedCache;
    use super::{ReadThroughCache, FetchData, CacheStats};

    static mut FETCH_DATA_COUNT: u32 = 0;
    static mut GET_ERROR: u32 = 0;
    static mut WAIT: u32 = 0;

    #[test]
    fn simple_test_for_boundedcache() {
        let bcache: BoundedCache<u32, String> = BoundedCache::new(10);
        let _ = bcache.insert(10, "first".to_string()).unwrap();
        let _ = bcache.insert(20, "second".to_string()).unwrap();
        let r = bcache.get(&10);
        assert_eq!(r.unwrap(), "first".to_string());
        let r1 = bcache.get(&20);
        assert_eq!(r1.unwrap(), "second".to_string());
        let r2 = bcache.get(&30);
        assert_eq!(r2, None);
    }
    #[test]
    fn bound_test() {
        let bcache: BoundedCache<u32, String> = BoundedCache::new(2);
        let _ = bcache.insert(10, "first".to_string()).unwrap();
        let _ = bcache.insert(20, "second".to_string()).unwrap();
        let r = bcache.insert(30, "third".to_string());
        assert!(r.is_err());
        let r1 = bcache.get(&30);
        assert_eq!(r1, None);
    }

    pub struct FetchMetaData;

    #[async_trait]
    impl FetchData<String, String, u64> for FetchMetaData {
        async fn fetch_data(&self, key: String, _size: u64) -> Result<String, Error> {
            unsafe {
                FETCH_DATA_COUNT += 1;
                if GET_ERROR == 1 {
                    return Err(Error::new(CacheError::Unknown("API Failed".to_string())))
                }
                if WAIT == 1 {
                    sleep(Duration::from_millis(100)).await;
                }
            }
            let mut database = HashMap::new();
            database.insert("key1".to_string(), "value1".to_string());
            database.insert("key2".to_string(), "value2".to_string());
            database.insert("key3".to_string(), "value3".to_string());
            let res = database.remove(&key).unwrap();
            return Ok(res);
        }
    }

    async fn cache_start(rt: Arc<Runtime>, mcache: Arc<ReadThroughCache<String, String, u64>>) {
        mcache.start(rt).await;
    }

    async fn cache_simple_get(key: String, mcache: Arc<ReadThroughCache<String, String, u64>>) -> Result<String, Error> {
        let data = mcache.get(key, 1024).await;
        data
    }

    async fn cache_stats(mcache: Arc<ReadThroughCache<String, String, u64>>) -> Result<CacheStats, Error> {
        mcache.stats().await
    }

    async fn cache_exit(mcache: Arc<ReadThroughCache<String, String, u64>>) {
        let _ = mcache.exit().await;
    }

    async fn wait_for_joinhandle(handle_list: Vec<JoinHandle<Result<String, Error>>>) -> Vec<String> {
        let mut res = Vec::new();
        for jh in handle_list {
            let val = jh.await.unwrap().unwrap();
            res.push(val);
        }
        res
    }

    fn simple_test_for_read_through_cache() {
        unsafe {
            FETCH_DATA_COUNT = 0;
            WAIT = 0;
            GET_ERROR = 0;
        }
        let rt = Arc::new(Runtime::new().unwrap());
        let mcache = Arc::new(ReadThroughCache::<String, String, u64>::new(10, Arc::new(FetchMetaData{})));
        let srt = rt.clone();
        rt.block_on(cache_start(srt.clone(), mcache.clone()));
        let res = rt.block_on(cache_simple_get(String::from("key1"), mcache.clone())).unwrap();
        assert_eq!(res, String::from("value1"));
        unsafe {
            assert_eq!(FETCH_DATA_COUNT, 1);
        }
        rt.block_on(cache_exit(mcache.clone()));
    }
    
    fn concurrent_cached_test_with_same_keys() {
        unsafe {
            FETCH_DATA_COUNT = 0;
            WAIT = 1;
            GET_ERROR = 0;
        }
        let rt = Arc::new(Runtime::new().unwrap());
        let mcache = Arc::new(ReadThroughCache::<String, String, u64>::new(10, Arc::new(FetchMetaData{})));
        let srt = rt.clone();
        rt.block_on(cache_start(srt.clone(), mcache.clone()));
        let mut handle_list = Vec::new();
        for _x in 0..4 {
            let res = rt.spawn(cache_simple_get(String::from("key1"), mcache.clone()));
            handle_list.push(res);
        }
        let res = rt.block_on(wait_for_joinhandle(handle_list));
        let stats = rt.block_on(cache_stats(mcache.clone())).unwrap();
        assert_eq!(stats.cache_inflight_hit_count.load(Ordering::SeqCst), 3);
        assert_eq!(stats.cache_miss_count.load(Ordering::SeqCst), 1);
        assert_eq!(res[0], res[2]);
        assert_eq!(res[1], res[3]);
        unsafe {
            assert_eq!(FETCH_DATA_COUNT, 1);
        }
        rt.block_on(cache_exit(mcache));
    }

    fn concurrent_cached_test_with_different_keys() {
        unsafe {
            FETCH_DATA_COUNT = 0;
            WAIT = 1;
            GET_ERROR = 0;
        }
        let rt = Arc::new(Runtime::new().unwrap());
        let mcache = Arc::new(ReadThroughCache::<String, String, u64>::new(10, Arc::new(FetchMetaData{})));
        let srt = rt.clone();
        rt.block_on(cache_start(srt.clone(), mcache.clone()));
        let jh1 = rt.spawn(cache_simple_get(String::from("key1"), mcache.clone()));
        let jh2 = rt.spawn(cache_simple_get(String::from("key2"), mcache.clone()));
        let jh3 = rt.spawn(cache_simple_get(String::from("key3"), mcache.clone()));
        let jh4 = rt.spawn(cache_simple_get(String::from("key1"), mcache.clone()));
        let handle_list = vec![jh1, jh2, jh3, jh4];
        let res = rt.block_on(wait_for_joinhandle(handle_list));
        assert_eq!(res[0], res[3]);
        unsafe {
            assert_eq!(FETCH_DATA_COUNT, 3);
        }
        let stats = rt.block_on(cache_stats(mcache.clone())).unwrap();
        assert_eq!(stats.cache_inflight_hit_count.load(Ordering::SeqCst), 1);
        assert_eq!(stats.cache_miss_count.load(Ordering::SeqCst), 3);
        rt.block_on(cache_exit(mcache.clone()));
    }

    fn eviction_test_for_read_through_cache() {
        unsafe {
            FETCH_DATA_COUNT = 0;
            WAIT = 0;
            GET_ERROR = 0;
        }
        let rt = Arc::new(Runtime::new().unwrap());
        let mcache = Arc::new(ReadThroughCache::<String, String, u64>::new(2, Arc::new(FetchMetaData{})));
        let srt = rt.clone();
        rt.block_on(cache_start(srt.clone(), mcache.clone()));
        let _res = rt.block_on(cache_simple_get(String::from("key1"), mcache.clone()));
        let _res = rt.block_on(cache_simple_get(String::from("key2"), mcache.clone()));
        let _res = rt.block_on(cache_simple_get(String::from("key3"), mcache.clone()));
        unsafe {
            assert_eq!(FETCH_DATA_COUNT, 3);
        }
        let _res = rt.block_on(cache_simple_get(String::from("key2"), mcache.clone()));
        unsafe {
            assert_eq!(FETCH_DATA_COUNT, 3);
        }
        let _res = rt.block_on(cache_simple_get(String::from("key1"), mcache.clone()));
        unsafe {
            assert_eq!(FETCH_DATA_COUNT, 4);
        }
        rt.block_on(cache_exit(mcache.clone()));
    }

    fn error_test_for_read_through_cache() {
        unsafe {
            FETCH_DATA_COUNT = 0;
            WAIT = 0;
            GET_ERROR = 1;
        }
        let rt = Arc::new(Runtime::new().unwrap());
        let mcache = Arc::new(ReadThroughCache::<String, String, u64>::new(10, Arc::new(FetchMetaData{})));
        let srt = rt.clone();
        rt.block_on(cache_start(srt.clone(), mcache.clone()));
        let res = rt.block_on(cache_simple_get(String::from("key1"), mcache.clone()));
        unsafe {
            assert_eq!(FETCH_DATA_COUNT, 1);
        }
        assert_eq!(res.unwrap_err().to_string(), Error::new(CacheError::Unknown("API Failed".to_string())).to_string());
        rt.block_on(cache_exit(mcache.clone()));
    }

    fn error_recovery_test_for_read_through_cache() {
        unsafe {
            FETCH_DATA_COUNT = 0;
            WAIT = 0;
            GET_ERROR = 1;
        }
        let rt = Arc::new(Runtime::new().unwrap());
        let mcache = Arc::new(ReadThroughCache::<String, String, u64>::new(10, Arc::new(FetchMetaData{})));
        let srt = rt.clone();
        rt.block_on(cache_start(srt.clone(), mcache.clone()));
        let res = rt.block_on(cache_simple_get(String::from("key1"), mcache.clone()));
        unsafe {
            assert_eq!(FETCH_DATA_COUNT, 1);
        }
        assert_eq!(res.unwrap_err().to_string(), Error::new(CacheError::Unknown("API Failed".to_string())).to_string());
        unsafe {
            GET_ERROR = 0;
        }
        let res1 = rt.block_on(cache_simple_get(String::from("key1"), mcache.clone()));
        let res2 = rt.block_on(cache_simple_get(String::from("key2"), mcache.clone()));
        assert_eq!(res1.unwrap(), String::from("value1"));
        assert_eq!(res2.unwrap(), String::from("value2"));
        rt.block_on(cache_exit(mcache.clone()));
    }

    fn test_submit_request_fail_read_through_cache() {
        unsafe {
            FETCH_DATA_COUNT = 0;
            WAIT = 0;
            GET_ERROR = 0;
        }
        let rt = Arc::new(Runtime::new().unwrap());
        let mcache = Arc::new(ReadThroughCache::<String, String, u64>::new(10, Arc::new(FetchMetaData{})));
        let (_tx, rx) = tokio::sync::mpsc::channel(10);
        let limit = mcache.limit;
        rt.spawn(async move { let _ = ReadThroughCache::<String, String, u64>::cache_loop(rx, limit).await; });
        let res = rt.block_on(cache_simple_get(String::from("key1"), mcache.clone()));
        assert!(res.is_err());
        rt.block_on(cache_exit(mcache.clone()));
    }

    fn test_submit_request_fail_read_through_cache_channel_close() {
        unsafe {
            FETCH_DATA_COUNT = 0;
            WAIT = 0;
            GET_ERROR = 0;
        }
        let rt = Arc::new(Runtime::new().unwrap());
        let mcache = Arc::new(ReadThroughCache::<String, String, u64>::new(10, Arc::new(FetchMetaData{})));
        let (tx, mut rx) = tokio::sync::mpsc::channel(10);
        let limit = mcache.limit;
        let _ = mcache.tx.set(tx);
        rx.close();
        rt.spawn(async move { let _ = ReadThroughCache::<String, String, u64>::cache_loop(rx, limit).await; });
        let res = rt.block_on(cache_simple_get(String::from("key1"), mcache.clone()));
        assert!(res.is_err());
        rt.block_on(cache_exit(mcache.clone()));
    }

    #[test]
    fn test_read_through_cache() {
        simple_test_for_read_through_cache();
        eviction_test_for_read_through_cache();
        error_test_for_read_through_cache();
        concurrent_cached_test_with_different_keys();
        concurrent_cached_test_with_same_keys();
        error_recovery_test_for_read_through_cache();
    }

    #[test]
    fn run_test_submit_request_fail_read_through_cache() {
        test_submit_request_fail_read_through_cache();
    }

    #[test]
    fn run_test_submit_request_fail_read_through_cache_channel_close() {
        test_submit_request_fail_read_through_cache_channel_close();
    }
}
