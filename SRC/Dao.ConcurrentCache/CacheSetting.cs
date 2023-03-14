using System;
using System.Linq;

namespace Dao.ConcurrentCache
{
    public class CacheSetting
    {
        public bool AcceptDefaultValue { get; set; } = true;

        TimeSpan? absoluteExpiration;
        public TimeSpan? AbsoluteExpiration
        {
            get => this.absoluteExpiration;
            set
            {
                this.absoluteExpiration = value;
                this.hasAbsoluteExpiration = null;
                ClearCache();
            }
        }

        TimeSpan? relativeExpiration;
        public TimeSpan? RelativeExpiration
        {
            get => this.relativeExpiration;
            set
            {
                this.relativeExpiration = value;
                this.hasRelativeExpiration = null;
                ClearCache();
            }
        }

        void ClearCache()
        {
            this.isPermanent = null;
            this.interval = null;
        }

        bool? hasAbsoluteExpiration;

        internal bool HasAbsoluteExpiration =>
            (this.hasAbsoluteExpiration = this.hasAbsoluteExpiration ?? (AbsoluteExpiration != null && AbsoluteExpiration > TimeSpan.Zero)).Value;

        bool? hasRelativeExpiration;
        internal bool HasRelativeExpiration =>
            (this.hasRelativeExpiration = this.hasRelativeExpiration ?? (RelativeExpiration != null && RelativeExpiration > TimeSpan.Zero)).Value;

        bool? isPermanent;
        internal bool IsPermanent =>
            (this.isPermanent = this.isPermanent ?? (!HasAbsoluteExpiration && !HasRelativeExpiration)).Value;

        double? interval;
        internal double Interval =>
            (this.interval = this.interval ?? (IsPermanent
                ? 0
                : Math.Min(86400000, Math.Max(1000, new[] { AbsoluteExpiration, RelativeExpiration }.Where(w => w != null).Select(s => s.Value).Min().TotalMilliseconds / 10)))).Value;
    }
}