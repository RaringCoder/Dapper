using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection;
using Dapper.Contrib.Extensions;
#if NETSTANDARD1_3
using DataException = System.InvalidOperationException;
#endif

namespace Dapper.Contrib
{
    internal sealed class TypeProperties
    {
        public PropertyInfo[] KeyProperties { get; }
        public PropertyInfo[] ExplicitKeyProperties { get; }
        public PropertyInfo[] AllProperties { get; }
        public PropertyInfo[] ComputedProperties { get; }
        public PropertyInfo RowVersionProperty { get; }

        public TypeProperties(
            PropertyInfo[] keyProperties,
            PropertyInfo[] explicitKeyProperties,
            PropertyInfo[] allProperties,
            PropertyInfo[] computedProperties,
            PropertyInfo rowVersionProperty)
        {
            KeyProperties = keyProperties;
            ExplicitKeyProperties = explicitKeyProperties;
            AllProperties = allProperties;
            ComputedProperties = computedProperties;
            RowVersionProperty = rowVersionProperty;
        }
    }



    internal static class TypeCache
    {
        private static readonly ConcurrentDictionary<RuntimeTypeHandle, TypeProperties> CachedTypes =
            new ConcurrentDictionary<RuntimeTypeHandle, TypeProperties>();

        private static TypeProperties WalkTypeProperties(Type type)
        {
            var allProperties = type.GetProperties();

            var properties = new List<PropertyInfo>(allProperties.Length);
            var keys = new List<PropertyInfo>();
            var explicitKeys = new List<PropertyInfo>();
            var computed = new List<PropertyInfo>();
            
            PropertyInfo rowVersion = null;
            PropertyInfo idPropertyByConvention = null;

            foreach (var property in allProperties.Where(IsWriteable))
            {
                properties.Add(property);

                var attributes = property.GetCustomAttributes(true);

                bool propertyHasExplicitKey = false;

                foreach (var attribute in attributes)
                {
                    if (attribute is KeyAttribute)
                    {
                        keys.Add(property);
                    }
                    else if (attribute is ExplicitKeyAttribute)
                    {
                        explicitKeys.Add(property);
                        propertyHasExplicitKey = true;
                    }
                    else if (attribute is ComputedAttribute)
                    {
                        computed.Add(property);
                    }
                    else if (attribute is RowVersionAttribute)
                    {
                        if (rowVersion != null)
                        {
                            throw new DataException($"Only one property can be decorated with [RowVersion].");
                        }

                        rowVersion = property;
                    }
                }

                // If we have not yet found the convention-based Id and we have no regular keys and this property isn't an explicit key, keep searching.
                if (idPropertyByConvention == null && keys.Count == 0 && !propertyHasExplicitKey)
                {
                    if (string.Equals(property.Name, "id", StringComparison.CurrentCultureIgnoreCase))
                    {
                        idPropertyByConvention = property;
                    }
                }
            }

            if (keys.Count == 0 && idPropertyByConvention != null)
            {
                keys.Add(idPropertyByConvention);
            }

            // Capture arrays so we don't waste unallocated space in the lists, as we are caching for the duration of the process.
            // TODO When Spans come out, we can allocate one large array of PropertyInfo[] and span across for specific properties.
            var cache = new TypeProperties(
                keys.ToArray(),
                explicitKeys.ToArray(),
                properties.ToArray(),
                computed.ToArray(),
                rowVersion);

            CachedTypes[type.TypeHandle] = cache;
            return cache;
        }

        internal static PropertyInfo RowVersionPropertyCache(Type type)
        {
            if (CachedTypes.TryGetValue(type.TypeHandle, out TypeProperties properties))
            {
                return properties.RowVersionProperty;
            }

            var cache = WalkTypeProperties(type);
            return cache.RowVersionProperty;
        }

        internal static PropertyInfo[] ComputedProperties(Type type)
        {
            if (CachedTypes.TryGetValue(type.TypeHandle, out TypeProperties properties))
            {
                return properties.ComputedProperties;
            }

            var cache = WalkTypeProperties(type);
            return cache.ComputedProperties;
        }

        internal static PropertyInfo[] ExplicitKeyProperties(Type type)
        {
            if (CachedTypes.TryGetValue(type.TypeHandle, out TypeProperties properties))
            {
                return properties.ExplicitKeyProperties;
            }

            var cache = WalkTypeProperties(type);
            return cache.ExplicitKeyProperties;
        }

        internal static PropertyInfo[] KeyProperties(Type type)
        {
            if (CachedTypes.TryGetValue(type.TypeHandle, out TypeProperties properties))
            {
                return properties.KeyProperties;
            }

            var cache = WalkTypeProperties(type);
            return cache.KeyProperties;
        }

        internal static PropertyInfo[] AllProperties(Type type)
        {
            if (CachedTypes.TryGetValue(type.TypeHandle, out TypeProperties properties))
            {
                return properties.AllProperties;
            }

            var cache = WalkTypeProperties(type);
            return cache.AllProperties;
        }

        private static bool IsWriteable(PropertyInfo pi)
        {
            var attributes = pi.GetCustomAttributes(typeof(WriteAttribute), false).AsList();

            if (attributes.Count != 1)
            {
                return true;
            }

            var writeAttribute = (WriteAttribute)attributes[0];
            return writeAttribute.Write;
        }
    }
}
