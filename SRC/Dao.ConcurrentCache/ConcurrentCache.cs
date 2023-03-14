using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Dao.IndividualLock;
using Dao.QueueExecutor;

namespace Dao.ConcurrentCache
{
    public abstract class ConcurrentCache
    {
        protected static readonly CacheSetting defaultSetting = new CacheSetting();
        protected static readonly Task end = Task.FromResult(0);
    }

    public class ConcurrentCache<TKey, TValue> : ConcurrentCache
    {
        readonly ConcurrentDictionary<TKey, CacheEntry> cache;
        readonly IndividualLocks<TKey> locks;
        readonly Catcher catcher = new Catcher();

        #region Setting

        readonly CacheSetting setting;

        #endregion

        #region Constructor

        public ConcurrentCache() : this(null, null) { }

        public ConcurrentCache(IEqualityComparer<TKey> comparer = null, bool acceptDefaultValue = true, TimeSpan? absoluteExpiration = null, TimeSpan? relativeExpiration = null) : this(comparer, new CacheSetting
        {
            AcceptDefaultValue = acceptDefaultValue,
            AbsoluteExpiration = absoluteExpiration,
            RelativeExpiration = relativeExpiration
        }) { }

        public ConcurrentCache(IEqualityComparer<TKey> comparer = null, CacheSetting setting = null)
        {
            comparer = comparer ?? EqualityComparer<TKey>.Default;
            this.setting = setting ?? defaultSetting;

            this.cache = new ConcurrentDictionary<TKey, CacheEntry>(comparer);
            this.locks = new IndividualLocks<TKey>(comparer);
            this.catcher.Catch += CheckExpiration;
        }

        #endregion

        #region CheckExpiration

        void TriggerCheckExpiration()
        {
            if (!ShouldCheck(DateTime.UtcNow))
                return;

            this.catcher.Throw();
        }

        DateTime lastCheck;

        bool ShouldCheck(DateTime now) => !this.setting.IsPermanent && this.lastCheck.AddMilliseconds(this.setting.Interval) <= now;

        Task CheckExpiration()
        {
            var now = DateTime.UtcNow;
            if (!ShouldCheck(now))
                return end;

            var values = this.cache.Values.Where(w => w.IsExpired(this.setting, now)).ToList();
            values.ParallelForEach(entry => this.cache.TryRemove(entry.Key, out _));

            this.lastCheck = DateTime.UtcNow;
            return end;
        }

        #endregion

        #region publics

        public TValue Get(TKey key)
        {
            try
            {
                GetInternal(key, DateTime.UtcNow, out var value);
                return value;
            }
            finally
            {
                TriggerCheckExpiration();
            }
        }

        public bool Get(TKey key, out TValue value)
        {
            try
            {
                return GetInternal(key, DateTime.UtcNow, out value);
            }
            finally
            {
                TriggerCheckExpiration();
            }
        }

        bool GetInternal(TKey key, DateTime now, out TValue value)
        {
            if (this.cache.TryGetValue(key, out var entry))
                return GetCacheValue(entry, now, out value);

            value = default;
            return false;
        }

        bool GetCacheValue(CacheEntry entry, DateTime now, out TValue value)
        {
            if (entry == null || entry.IsExpired(this.setting, now))
            {
                value = default;
                if (entry != null)
                    this.cache.TryRemove(entry.Key, out _);
                return false;
            }

            value = entry.Value;
            return true;
        }

        public TValue GetOrSet(TKey key, Func<TKey, TValue> valueFactory)
        {
            try
            {
                return valueFactory == null
                    ? throw new ArgumentNullException(nameof(valueFactory))
                    : GetInternal(key, DateTime.UtcNow, out var value)
                        ? value
                        : this.cache.GetOrAdd(key, k => new CacheEntry(k, WrapValueFactory(k, valueFactory))).Value;
            }
            finally
            {
                TriggerCheckExpiration();
            }
        }

        Func<TValue> WrapValueFactory(TKey key, Func<TKey, TValue> valueFactory)
        {
            return () =>
            {
                try
                {
                    var value = valueFactory(key);
                    if (!this.setting.AcceptDefaultValue && EqualityComparer<TValue>.Default.Equals(value, default))
                        this.cache.TryRemove(key, out _);
                    return value;
                }
                catch (Exception)
                {
                    this.cache.TryRemove(key, out _);
                    throw;
                }
            };
        }

        public async Task<TValue> GetOrSetAsync(TKey key, Func<TKey, Task<TValue>> valueFactoryAsync)
        {
            try
            {
                if (valueFactoryAsync == null)
                    throw new ArgumentNullException(nameof(valueFactoryAsync));

                if (GetInternal(key, DateTime.UtcNow, out var value))
                    return value;

                using (await this.locks.LockAsync(key).ConfigureAwait(false))
                {
                    if (this.cache.TryGetValue(key, out var entry))
                        return entry.Value;

                    value = await valueFactoryAsync(key).ConfigureAwait(false);
                    return this.setting.AcceptDefaultValue || !EqualityComparer<TValue>.Default.Equals(value, default)
                        ? this.cache.GetOrAdd(key, k => new CacheEntry(k, () => value)).Value
                        : value;
                }
            }
            finally
            {
                TriggerCheckExpiration();
            }
        }

        public TValue Set(TKey key, Func<TKey, TValue> valueFactory)
        {
            try
            {
                if (valueFactory == null)
                    throw new ArgumentNullException(nameof(valueFactory));

                var entry = new CacheEntry(key, WrapValueFactory(key, valueFactory));
                this.cache[key] = entry;
                return entry.Value;
            }
            finally
            {
                TriggerCheckExpiration();
            }
        }

        public async Task<TValue> SetAsync(TKey key, Func<TKey, Task<TValue>> valueFactoryAsync)
        {
            try
            {
                if (valueFactoryAsync == null)
                    throw new ArgumentNullException(nameof(valueFactoryAsync));

                using (await this.locks.LockAsync(key).ConfigureAwait(false))
                {
                    var value = await valueFactoryAsync(key).ConfigureAwait(false);
                    if (this.setting.AcceptDefaultValue || !EqualityComparer<TValue>.Default.Equals(value, default))
                    {
                        var entry = new CacheEntry(key, () => value);
                        this.cache[key] = entry;
                    }

                    return value;
                }
            }
            finally
            {
                TriggerCheckExpiration();
            }
        }

        public void Remove(TKey key)
        {
            try
            {
                this.cache.TryRemove(key, out _);
            }
            finally
            {
                TriggerCheckExpiration();
            }
        }

        public ICollection<TKey> Keys
        {
            get
            {
                try
                {
                    return this.cache.Keys;
                }
                finally
                {
                    TriggerCheckExpiration();
                }
            }
        }

        public ICollection<TValue> Values
        {
            get
            {
                try
                {
                    return this.cache.Values.Select(s => s.Value).ToList().AsReadOnly();
                }
                finally
                {
                    TriggerCheckExpiration();
                }
            }
        }

        #endregion

        #region CacheItem

        class CacheEntry : Lazy<TValue>
        {
            public CacheEntry(TKey key, Func<TValue> valueFactory) : base(valueFactory) => Key = key;

            internal TKey Key { get; }

            internal new TValue Value
            {
                get
                {
                    AccessTime = DateTime.UtcNow;
                    return base.Value;
                }
            }

            DateTime CreateTime { get; } = DateTime.UtcNow;
            DateTime AccessTime { get; set; } = DateTime.UtcNow;

            internal bool IsExpired(CacheSetting setting, DateTime now) =>
                !setting.IsPermanent
                && ((setting.HasAbsoluteExpiration && CreateTime.AddMilliseconds(setting.AbsoluteExpiration.Value.TotalMilliseconds) <= now)
                    || (setting.HasRelativeExpiration && AccessTime.AddMilliseconds(setting.RelativeExpiration.Value.TotalMilliseconds) <= now));
        }

        #endregion
    }
}