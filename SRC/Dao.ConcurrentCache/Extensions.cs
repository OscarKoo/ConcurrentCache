using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Dao.ConcurrentCache
{
    public static class Extensions
    {
        public static void ParallelForEach<T>(this ICollection<T> source, Action<T> body)
        {
            if (source == null || source.Count <= 0 || body == null)
                return;

            if (source.Count == 1)
            {
                body(source.First());
                return;
            }

            Parallel.ForEach(source, body);
        }
    }
}