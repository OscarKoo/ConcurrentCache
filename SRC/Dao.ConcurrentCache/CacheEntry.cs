using System;
using Nito.AsyncEx;

namespace Dao.ConcurrentCache
{
    public class CacheEntry
    {
        public readonly AsyncLock Locker = new AsyncLock();
    }

    public class CacheEntry<T> : CacheEntry
    {
        public CacheObject<T> Entry { get; set; }
    }

    public class CacheObject<T>
    {
        public T Data { get; set; }
    }

    public class ExpirableCacheObject<T> : CacheObject<T>
    {
        public DateTime? AbsoluteExpiration { get; set; }
        public DateTime? RelativeExpiration { get; set; }
        public DateTime AccessTime = DateTime.Now;
        public readonly DateTime CreateTime = DateTime.Now;
    }
}