using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Dao.ConcurrentDictionaryLazy;

namespace Dao.ConcurrentCache
{
    public class ConcurrentCache<TKey, TValue>
    {
        readonly ConcurrentDictionaryLazy<TKey, CacheEntry<TValue>> cache;

        readonly bool acceptDefaultValue;
        readonly TimeSpan? absoluteExpiration;
        readonly TimeSpan? relativeExpiration;
        readonly TimeSpan? expiration;

        DateTime? lastExpirationCheck;
        bool isExpirationChecking;

        public ConcurrentCache(IEqualityComparer<TKey> comparer = null, bool acceptDefaultValue = true, TimeSpan? absoluteExpiration = null, TimeSpan? relativeExpiration = null)
        {
            if (comparer == null)
                comparer = EqualityComparer<TKey>.Default;

            this.cache = new ConcurrentDictionaryLazy<TKey, CacheEntry<TValue>>(comparer);

            this.acceptDefaultValue = acceptDefaultValue;
            this.absoluteExpiration = absoluteExpiration;
            this.relativeExpiration = relativeExpiration;
            this.expiration = new[] { absoluteExpiration, relativeExpiration }.Min();
        }

        #region ExpirationCheck

        void ExpirationCheck()
        {
            if (this.expiration == null)
                return;

            var expirationMS = this.expiration.Value.TotalMilliseconds;
            if (expirationMS <= 0)
                return;

            var now = DateTime.Now;

            // 轮询间隔为过期时间的一半, 最少1秒, 最多1天
            var checkInterval = Math.Min(86400000, Math.Max(1000, expirationMS / 10));

            if (!RequireExpirationCheck(checkInterval, now))
                return;

            lock (this.cache)
            {
                if (!RequireExpirationCheck(checkInterval, now))
                    return;

                this.isExpirationChecking = true;

                var keys = this.cache.Keys;

                keys.ParallelForEach(key =>
                {
                    CacheEntry<TValue> value;

                    if (!this.cache.TryGetValue(key, out value))
                        return;

                    var cacheObj = value.Entry as ExpirableCacheObject<TValue>;
                    if (cacheObj == null)
                        return;

                    if (cacheObj.AbsoluteExpiration != null && cacheObj.AbsoluteExpiration.Value < now
                        || cacheObj.RelativeExpiration != null && cacheObj.RelativeExpiration.Value < now)
                    {
                        this.cache.Remove(key);
                    }
                });

                this.lastExpirationCheck = now;
                this.isExpirationChecking = false;
            }
        }

        bool RequireExpirationCheck(double checkInterval, DateTime now)
        {
            return !this.isExpirationChecking && (this.lastExpirationCheck == null || this.lastExpirationCheck.Value.AddMilliseconds(checkInterval) < now);
        }

        #endregion

        #region GetOrAdd

        public TValue GetOrAdd(TKey key, Func<TKey, TValue> valueFactory)
        {
            var entry = GetEntry(key);

            var checkPoint = DateTime.Now;
            CacheObject<TValue> cacheObj;
            if (!HasCache(entry, checkPoint, out cacheObj))
            {
                cacheObj = AddOrUpdate(true, entry, key, checkPoint, valueFactory);
            }

            Task.Run((Action)ExpirationCheck);

            return cacheObj.Data;
        }

        CacheEntry<TValue> GetEntry(TKey key)
        {
            return this.cache.GetOrAdd(key, k => new CacheEntry<TValue>());
        }

        bool HasCache(CacheEntry<TValue> entry, DateTime now, out CacheObject<TValue> value)
        {
            if (entry.Entry == null)
            {
                value = null;
                return false;
            }

            var cacheObj = entry.Entry as ExpirableCacheObject<TValue>;

            if (cacheObj != null)
            {
                if (cacheObj.AbsoluteExpiration != null && cacheObj.AbsoluteExpiration < now
                    || cacheObj.RelativeExpiration != null && cacheObj.RelativeExpiration < now)
                {
                    value = null;
                    return false;
                }

                cacheObj.AccessTime = DateTime.Now;
                RefreshRelativeExpiration(cacheObj);
            }

            value = entry.Entry;
            return true;
        }

        void RefreshRelativeExpiration(ExpirableCacheObject<TValue> cacheObj)
        {
            if (this.relativeExpiration != null)
                cacheObj.RelativeExpiration = cacheObj.AccessTime.AddMilliseconds(this.relativeExpiration.Value.TotalMilliseconds);
        }

        CacheObject<TValue> AddOrUpdate(bool requireCheck, CacheEntry<TValue> entry, TKey key, DateTime checkPoint, Func<TKey, TValue> valueFactory)
        {
            CacheObject<TValue> cacheObj;
            using (entry.Locker.Lock())
            {
                if (requireCheck && HasCache(entry, checkPoint, out cacheObj))
                    return cacheObj;

                entry.Entry = null;

                cacheObj = this.expiration == null
                    ? new CacheObject<TValue>()
                    : new ExpirableCacheObject<TValue>();

                try
                {
                    var value = valueFactory(key);
                    SetValue(entry, cacheObj, value);
                }
                catch (Exception ex) { }
            }

            return cacheObj;
        }

        void SetValue(CacheEntry<TValue> entry, CacheObject<TValue> cacheObj, TValue value)
        {
            cacheObj.Data = value;

            if (!this.acceptDefaultValue && Equals(value, default(TValue)))
                return;

            ExpirableCacheObject<TValue> expirableCacheObject;
            if (this.expiration != null
                && (expirableCacheObject = cacheObj as ExpirableCacheObject<TValue>) != null)
            {
                if (this.absoluteExpiration != null && expirableCacheObject.AbsoluteExpiration == null)
                    expirableCacheObject.AbsoluteExpiration = expirableCacheObject.CreateTime.AddMilliseconds(this.absoluteExpiration.Value.TotalMilliseconds);

                RefreshRelativeExpiration(expirableCacheObject);
            }

            entry.Entry = cacheObj;
        }

        public async Task<TValue> GetOrAddAsync(TKey key, Func<TKey, Task<TValue>> valueFactory)
        {
            var entry = GetEntry(key);

            var checkPoint = DateTime.Now;
            CacheObject<TValue> cacheObj;
            if (!HasCache(entry, checkPoint, out cacheObj))
            {
                cacheObj = await AddOrUpdateAsync(true, entry, key, checkPoint, valueFactory);
            }

            Task.Run((Action)ExpirationCheck);

            return cacheObj.Data;
        }

        async Task<CacheObject<TValue>> AddOrUpdateAsync(bool requireCheck, CacheEntry<TValue> entry, TKey key, DateTime checkPoint, Func<TKey, Task<TValue>> valueFactory)
        {
            CacheObject<TValue> cacheObj;
            using (await entry.Locker.LockAsync())
            {
                if (requireCheck && HasCache(entry, checkPoint, out cacheObj))
                    return cacheObj;

                entry.Entry = null;

                cacheObj = this.expiration == null
                    ? new CacheObject<TValue>()
                    : new ExpirableCacheObject<TValue>();

                try
                {
                    var value = await valueFactory(key);
                    SetValue(entry, cacheObj, value);
                }
                catch (Exception ex) { }
            }

            return cacheObj;
        }

        #endregion

        #region TryGet/Update

        /// <summary>
        /// Possible return default(<see cref="TValue"/>) value
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        public TValue GetValue(TKey key)
        {
            CacheEntry<TValue> entry;
            CacheObject<TValue> cacheObj;

            return TryGetValue(key, false, out entry, out cacheObj)
                ? cacheObj.Data
                : default(TValue);
        }

        bool TryGetValue(TKey key, bool createKey, out CacheEntry<TValue> entry, out CacheObject<TValue> cacheObj)
        {
            if (this.cache.TryGetValue(key, out entry))
            {
                if (HasCache(entry, DateTime.Now, out cacheObj))
                    return true;
            }
            else
            {
                if (createKey)
                    entry = GetEntry(key);
            }

            cacheObj = null;
            return false;
        }

        public void UpdateValue(TKey key, Func<TKey, TValue, TValue> updateFactory, Func<TKey, TValue> addFactory = null)
        {
            CacheEntry<TValue> entry;
            CacheObject<TValue> cacheObj;

            var hasCache = TryGetValue(key, true, out entry, out cacheObj);
            if (!hasCache && addFactory == null)
                return;

            AddOrUpdate(false, entry, key, DateTime.Now, k => !hasCache ? addFactory(k) : updateFactory(key, cacheObj.Data));

            Task.Run((Action)ExpirationCheck);
        }

        public async Task UpdateValueAsync(TKey key, Func<TKey, TValue, Task<TValue>> updateFactory, Func<TKey, Task<TValue>> addFactory = null)
        {
            CacheEntry<TValue> entry;
            CacheObject<TValue> cacheObj;

            var hasCache = TryGetValue(key, true, out entry, out cacheObj);
            if (!hasCache && addFactory == null)
                return;

            await AddOrUpdateAsync(false, entry, key, DateTime.Now, async k => !hasCache ? await addFactory(k) : await updateFactory(key, cacheObj.Data));
            
            Task.Run((Action)ExpirationCheck);
        }

        #endregion

        #region Keys/Values

        public IEnumerable<TKey> Keys
        {
            get
            {
                return this.cache
                    .Where(w => w.Value.Entry != null)
                    .Select(s => s.Key);
            }
        }

        public IEnumerable<TValue> Values
        {
            get
            {
                return this.cache.Values
                    .Where(w => w.Entry != null)
                    .Select(s => s.Entry.Data);
            }
        }

        #endregion

        public void Remove(TKey key)
        {
            this.cache.Remove(key);
        }
    }
}