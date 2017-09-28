using System;
using System.Collections.Concurrent;

namespace Dapper.Contrib
{
    internal static class SqlCache
    {
        private static readonly ConcurrentDictionary<RuntimeTypeHandle, string> Selects =
            new ConcurrentDictionary<RuntimeTypeHandle, string>();

        // TODO Can't currently do Inserts because of how the ISqlAdapter is designed.
        //private static readonly ConcurrentDictionary<RuntimeTypeHandle, string> Inserts =
        //    new ConcurrentDictionary<RuntimeTypeHandle, string>();

        private static readonly ConcurrentDictionary<RuntimeTypeHandle, string> Updates =
            new ConcurrentDictionary<RuntimeTypeHandle, string>();

        private static readonly ConcurrentDictionary<RuntimeTypeHandle, string> Deletes =
            new ConcurrentDictionary<RuntimeTypeHandle, string>();


        internal static string GetOrCacheSelect(Type type, Func<string> getSelect)
        {
            return GetOrCache(type, getSelect, Selects);
        }

        //internal static string GetOrCacheInsert(Type type, Func<string> getInsert)
        //{
        //    return GetOrCache(type, getInsert, Inserts);
        //}

        internal static string GetOrCacheUpdate(Type type, Func<string> getUpdate)
        {
            return GetOrCache(type, getUpdate, Updates);
        }

        internal static string GetOrCacheDelete(Type type, Func<string> getDelete)
        {
            return GetOrCache(type, getDelete, Deletes);
        }

        private static string GetOrCache(
            Type type,
            Func<string> getStatement,
            ConcurrentDictionary<RuntimeTypeHandle, string> cache)
        {
            if (!cache.TryGetValue(type.TypeHandle, out string sql))
            {
                sql = getStatement();
                cache[type.TypeHandle] = sql;
            }

            return sql;
        }
    }
}
