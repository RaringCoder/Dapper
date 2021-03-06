﻿using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Reflection.Emit;
#if NETSTANDARD1_3
using DataException = System.InvalidOperationException;
#else
using System.Threading;
#endif

namespace Dapper.Contrib.Extensions
{
    /// <summary>
    /// The Dapper.Contrib extensions for Dapper
    /// </summary>
    public static partial class SqlMapperExtensions
    {
        /// <summary>
        /// Defined a proxy object with a possibly dirty state.
        /// </summary>
        public interface IProxy //must be kept public
        {
            /// <summary>
            /// Whether the object has been changed.
            /// </summary>
            bool IsDirty { get; set; }
        }

        /// <summary>
        /// Defines a table name mapper for getting table names from types.
        /// </summary>
        public interface ITableNameMapper
        {
            /// <summary>
            /// Gets a table name from a given <see cref="Type"/>.
            /// </summary>
            /// <param name="type">The <see cref="Type"/> to get a name from.</param>
            /// <returns>The table name for the given <paramref name="type"/>.</returns>
            string GetTableName(Type type);
        }

        /// <summary>
        /// The function to get a database type from the given <see cref="IDbConnection"/>.
        /// </summary>
        /// <param name="connection">The connection to get a database type name from.</param>
        public delegate string GetDatabaseTypeDelegate(IDbConnection connection);

        /// <summary>
        /// The function to get a a table name from a given <see cref="Type"/>
        /// </summary>
        /// <param name="type">The <see cref="Type"/> to get a table name for.</param>
        public delegate string TableNameMapperDelegate(Type type);

        private static readonly ISqlAdapter DefaultAdapter = new SqlServerAdapter();

        private static readonly Dictionary<string, ISqlAdapter> AdapterDictionary
            = new Dictionary<string, ISqlAdapter>
            {
                ["sqlconnection"] = new SqlServerAdapter(),
                ["sqlceconnection"] = new SqlCeServerAdapter(),
                ["npgsqlconnection"] = new PostgresAdapter(),
                ["sqliteconnection"] = new SQLiteAdapter(),
                ["mysqlconnection"] = new MySqlAdapter(),
                ["fbconnection"] = new FirebaseAdapter()
            };

        internal static readonly ConcurrentDictionary<RuntimeTypeHandle, string> TypeTableName =
            new ConcurrentDictionary<RuntimeTypeHandle, string>();


        private static PropertyInfo GetSingleKey<T>(string method)
        {
            var type = typeof(T);
            var keys = TypeCache.KeyProperties(type);
            var explicitKeys = TypeCache.ExplicitKeyProperties(type);
            var keyCount = keys.Length + explicitKeys.Length;

            if (keyCount > 1)
            {
                throw new DataException($"{method}<T> only supports an entity with a single [Key] or [ExplicitKey] property");
            }

            if (keyCount == 0)
            {
                throw new DataException($"{method}<T> only supports an entity with a [Key] or an [ExplicitKey] property");
            }

            return keys.Length > 0 ? keys[0] : explicitKeys[0];
        }

        /// <summary>
        /// Returns a single entity by a single id from table "Ts".  
        /// Id must be marked with [Key] attribute.
        /// Entities created from interfaces are tracked/intercepted for changes and used by the Update() extension
        /// for optimal performance. 
        /// </summary>
        /// <typeparam name="T">Interface or type to create and populate</typeparam>
        /// <param name="connection">Open SqlConnection</param>
        /// <param name="id">Id of the entity to get, must be marked with [Key] attribute</param>
        /// <param name="transaction">The transaction to run under, null (the default) if none</param>
        /// <param name="commandTimeout">Number of seconds before command execution timeout</param>
        /// <returns>Entity of T</returns>
        public static T Get<T>(
            this IDbConnection connection, 
            dynamic id, 
            IDbTransaction transaction = null,
            int? commandTimeout = null) where T : class
        {
            var type = typeof(T);

            var sql = GetSelectScript<T>(type, nameof(Get));

            var dynParms = new DynamicParameters();
            dynParms.Add("@id", id);

            if (!type.IsInterface())
            {
                var results = connection
                    .Query<T>(sql, dynParms, transaction, commandTimeout: commandTimeout);

                return results.FirstOrDefault();
            }
            
            return MapResultToType<T>(
                connection.Query(sql, dynParms, transaction, commandTimeout: commandTimeout),
                type);
        }

        private static string GetSelectScript<T>(Type type, string methodName)
        {
            var sql = SqlCache.GetOrCacheSelect(type, () =>
            {
                // We can be sly and cache or make use of the SelectAll stuff on Select.
                var selectFromTable = GetSelectAllScript<T>(methodName);

                var key = GetSingleKey<T>(methodName);
                return $"{selectFromTable} WHERE {key.Name} = @id";
            });

            return sql;
        }

        /// <summary>
        /// Returns a list of entites from table "Ts".  
        /// Id of T must be marked with [Key] attribute.
        /// Entities created from interfaces are tracked/intercepted for changes and used by the Update() extension
        /// for optimal performance. 
        /// </summary>
        /// <typeparam name="T">Interface or type to create and populate</typeparam>
        /// <param name="connection">Open SqlConnection</param>
        /// <param name="transaction">The transaction to run under, null (the default) if none</param>
        /// <param name="commandTimeout">Number of seconds before command execution timeout</param>
        /// <returns>Entity of T</returns>
        public static IEnumerable<T> GetAll<T>(
            this IDbConnection connection, 
            IDbTransaction transaction = null,
            int? commandTimeout = null) where T : class
        {
            var type = typeof(T);
            GetSingleKey<T>(nameof(GetAll));
            var sql = GetSelectAllScript<T>(nameof(GetAll));

            if (!type.IsInterface())
            {
                return connection.Query<T>(sql, transaction, commandTimeout: commandTimeout);
            }

            var result = connection.Query(sql, transaction: transaction, commandTimeout: commandTimeout);
            return MapResultsToType<T>(result, type);
        }
        
        private static string GetSelectAllScript<T>(string methodName)
        {
            var selectFromTable = SqlCache.GetOrCacheSelect(typeof(List<T>), () =>
            {
                var type = typeof(T);
                GetSingleKey<T>(methodName); // assert single key
                var name = GetTableName(type);
                var columns = GetSelectStatementColumns(type);

                return $"SELECT {columns} FROM {name}";
            });

            return selectFromTable;
        }

        private static T MapResultToType<T>(IEnumerable<dynamic> results, Type type)
            where T : class
        {
            if (!(results.FirstOrDefault() is IDictionary<string, object> res))
            {
                return null;
            }

            var obj = CreateObject<T>(type, res);
            return obj;
        }

        private static IEnumerable<T> MapResultsToType<T>(IEnumerable<dynamic> result, Type type)
        {
            var list = new List<T>();

            foreach (IDictionary<string, object> res in result)
            {
                var obj = CreateObject<T>(type, res);
                list.Add(obj);
            }

            return list;
        }

        private static T CreateObject<T>(Type type, IDictionary<string, object> res)
        {
            var obj = ProxyGenerator.GetInterfaceProxy<T>();

            foreach (var property in TypeCache.AllProperties(type))
            {
                var val = res[property.Name];
                property.SetValue(obj, Convert.ChangeType(val, property.PropertyType), null);
            }

            ((IProxy)obj).IsDirty = false; //reset change tracking and return
            return obj;
        }

        /// <summary>
        /// Specify a custom table name mapper based on the POCO type name
        /// </summary>
        public static TableNameMapperDelegate TableNameMapper;

        private static string GetTableName(Type type)
        {
            if (TypeTableName.TryGetValue(type.TypeHandle, out string name))
            {
                return name;
            }

            if (TableNameMapper != null)
            {
                name = TableNameMapper(type);
            }
            else
            {
                //NOTE: This as dynamic trick should be able to handle both our own Table-attribute as well as the one in EntityFramework 
                var tableAttr = type
#if NETSTANDARD1_3
                    .GetTypeInfo()
#endif
                    .GetCustomAttributes(false)
                    .SingleOrDefault(attr => attr.GetType().Name == "TableAttribute") as dynamic;

                if (tableAttr != null)
                {
                    name = tableAttr.Name;
                }
                else
                {
                    name = type.Name + "s";

                    if (type.IsInterface() && name.StartsWith("I"))
                    {
                        name = name.Substring(1);
                    }
                }
            }

            TypeTableName[type.TypeHandle] = name;
            return name;
        }

        /// <summary>
        /// Inserts an entity into table "Ts" and returns identity id or number if inserted rows if inserting a list.
        /// </summary>
        /// <typeparam name="T">The type to insert.</typeparam>
        /// <param name="connection">Open SqlConnection</param>
        /// <param name="entityToInsert">Entity to insert, can be list of entities</param>
        /// <param name="transaction">The transaction to run under, null (the default) if none</param>
        /// <param name="commandTimeout">Number of seconds before command execution timeout</param>
        /// <returns>Identity of inserted entity, or number of inserted rows if inserting a list</returns>
        public static long Insert<T>(
            this IDbConnection connection, 
            T entityToInsert, 
            IDbTransaction transaction = null,
            int? commandTimeout = null) where T : class
        {
            if (entityToInsert == null)
            {
                throw new ArgumentNullException(nameof(entityToInsert), "Cannot insert a null object.");
            }

            var isList = false;

            var type = typeof(T);

            if (type.IsArray)
            {
                isList = true;
                type = type.GetElementType();
            }
            else if (type.IsGenericType())
            {
                isList = true;
                type = type.GetGenericArguments()[0];
            }

            var name = GetTableName(type);
            var allProperties = TypeCache.AllProperties(type);
            var keyProperties = TypeCache.KeyProperties(type);
            var computedProperties = TypeCache.ComputedProperties(type);
            var rowVersion = TypeCache.RowVersionPropertyCache(type);

            var allPropertiesExceptKeyComputedAndVersion = allProperties
                .Except(keyProperties.Union(computedProperties).Union(OptionalRowVersion(rowVersion)))
                .ToList();

            var adapter = GetFormatter(connection);
            
            var sbColumnList = new StringBuilder();
            var sbParameterList = new StringBuilder();

            for (var i = 0; i < allPropertiesExceptKeyComputedAndVersion.Count; i++)
            {
                if (i != 0)
                {
                    sbColumnList.Append(", ");
                    sbParameterList.Append(", ");
                }
                
                adapter.AppendColumnName(sbColumnList, allPropertiesExceptKeyComputedAndVersion[i].Name); //fix for issue #336
                sbParameterList.AppendFormat("@{0}", allPropertiesExceptKeyComputedAndVersion[i].Name);
            }

            int returnVal;
            var wasClosed = connection.State == ConnectionState.Closed;
            if (wasClosed)
            {
                connection.Open();
            }

            if (!isList) //single entity
            {
                returnVal = adapter.Insert(connection, transaction, commandTimeout, name, sbColumnList.ToString(),
                    sbParameterList.ToString(), keyProperties, entityToInsert);
            }
            else
            {
                //insert list of entities
                var cmd = $"INSERT INTO {name} ({sbColumnList}) VALUES ({sbParameterList})";
                returnVal = connection.Execute(cmd, entityToInsert, transaction, commandTimeout);
            }
            if (wasClosed)
            {
                connection.Close();
            }
            return returnVal;
        }
        
        /// <summary>
        /// Updates entity in table "Ts", checks if the entity is modified if the entity is tracked by the Get() extension.
        /// </summary>
        /// <typeparam name="T">Type to be updated</typeparam>
        /// <param name="connection">Open SqlConnection</param>
        /// <param name="entityToUpdate">Entity to be updated</param>
        /// <param name="transaction">The transaction to run under, null (the default) if none</param>
        /// <param name="commandTimeout">Number of seconds before command execution timeout</param>
        /// <returns>true if updated, false if not found or not modified (tracked entities)</returns>
        public static bool Update<T>(
            this IDbConnection connection, 
            T entityToUpdate, 
            IDbTransaction transaction = null,
            int? commandTimeout = null) where T : class
        {
            if (entityToUpdate == null)
            {
                throw new ArgumentNullException(nameof(entityToUpdate), "Cannot update a null object.");
            }

            if (entityToUpdate is IProxy proxy && !proxy.IsDirty)
            {
                return false;
            }

            var updateScript = GetUpdateScript<T>(connection);
            
            var updated = connection.Execute(
                updateScript,
                entityToUpdate, 
                commandTimeout: commandTimeout,
                transaction: transaction);
            
            return updated > 0;
        }

        /// <summary>
        /// Creates the update script agnostic of synchronous or asynchronous call paths.
        /// </summary>
        private static string GetUpdateScript<T>(IDbConnection connection)
        {
            var type = typeof(T);

            var sql = SqlCache.GetOrCacheUpdate(type, () =>
            {
                if (type.IsArray)
                {
                    type = type.GetElementType();
                }
                else if (type.IsGenericType())
                {
                    type = type.GetGenericArguments()[0];
                }

                var keyProperties = TypeCache.KeyProperties(type);
                var explicitKeyProperties = TypeCache.ExplicitKeyProperties(type);

                if (keyProperties.Length == 0 && explicitKeyProperties.Length == 0)
                {
                    throw new ArgumentException("Entity must have at least one [Key] or [ExplicitKey] property");
                }

                var adapter = GetFormatter(connection);

                var name = GetTableName(type);

                var sb = new StringBuilder();

                sb.AppendFormat("UPDATE {0} SET ", name);

                var allProperties = TypeCache.AllProperties(type);

                var rowVersion = TypeCache.RowVersionPropertyCache(type);

                var keysAndVersion = keyProperties
                    .Union(explicitKeyProperties)
                    .Union(OptionalRowVersion(rowVersion))
                    .ToList();

                var computedProperties = TypeCache.ComputedProperties(type);
                var nonUpdateProps = allProperties
                    .Except(keysAndVersion.Union(computedProperties))
                    .ToList();

                for (var i = 0; i < nonUpdateProps.Count; i++)
                {
                    var property = nonUpdateProps[i];
                    adapter.AppendColumnNameEqualsValue(sb, property.Name);

                    if (i < nonUpdateProps.Count - 1)
                    {
                        sb.AppendFormat(", ");
                    }
                }

                sb.Append(" WHERE ");

                for (int i = 0; i < keysAndVersion.Count; i++)
                {
                    if (i != 0)
                    {
                        sb.Append(" AND ");
                    }

                    adapter.AppendColumnNameEqualsValue(sb, keysAndVersion[i].Name);
                }

                return sb.ToString();
            });

            return sql;
        }

        private static IEnumerable<PropertyInfo> OptionalRowVersion(PropertyInfo rowVersion)
        {
            return rowVersion != null ? new[] {rowVersion} : Enumerable.Empty<PropertyInfo>();
        }

        private static bool RowVersionIsWriteable(PropertyInfo rowVersion)
        {
            if (rowVersion == null)
            {
                return false;
            }

            var att = rowVersion.GetCustomAttribute<WriteAttribute>(true);
            return att != null && att.Write;
        }

        internal static string GetSelectStatementColumns(Type type)
        {
            var properties = TypeCache.AllProperties(type)
                .Except(TypeCache.ComputedProperties(type))
                .ToList();

            var sb = new StringBuilder();

            for (int i = 0; i < properties.Count; i++)
            {
                if (i != 0)
                {
                    sb.Append(", ");
                }

                var property = properties[i];
                sb.AppendFormat("[{0}]", property.Name);
            }

            return sb.ToString();
        }

        /// <summary>
        /// Delete entity in table "Ts".
        /// </summary>
        /// <typeparam name="T">Type of entity</typeparam>
        /// <param name="connection">Open SqlConnection</param>
        /// <param name="entityToDelete">Entity to delete</param>
        /// <param name="transaction">The transaction to run under, null (the default) if none</param>
        /// <param name="commandTimeout">Number of seconds before command execution timeout</param>
        /// <returns>true if deleted, false if not found</returns>
        public static bool Delete<T>(
            this IDbConnection connection, 
            T entityToDelete, 
            IDbTransaction transaction = null,
            int? commandTimeout = null) where T : class
        {
            if (entityToDelete == null)
            {
                throw new ArgumentNullException(nameof(entityToDelete), "Cannot delete a null object.");
            }
            
            var deleteScript = GetDeleteScript<T>(connection);

            var deleted = connection.Execute(deleteScript, entityToDelete, transaction, commandTimeout);
            return deleted > 0;
        }

        private static string GetDeleteScript<T>(IDbConnection connection)
        {
            var type = typeof(T);

            var sql = SqlCache.GetOrCacheDelete(type, () =>
            {
                if (type.IsArray)
                {
                    type = type.GetElementType();
                }
                else if (type.IsGenericType())
                {
                    type = type.GetGenericArguments()[0];
                }

                var keyProperties = TypeCache.KeyProperties(type);
                var explicitKeyProperties = TypeCache.ExplicitKeyProperties(type);

                if (keyProperties.Length == 0 && explicitKeyProperties.Length == 0)
                {
                    throw new ArgumentException("Entity must have at least one [Key] or [ExplicitKey] property");
                }

                var name = GetTableName(type);

                var sb = new StringBuilder();
                sb.AppendFormat("DELETE FROM {0} WHERE ", name);

                var adapter = GetFormatter(connection);

                var totalKeys = keyProperties.Length + explicitKeyProperties.Length;
                int currentCount = 0;

                foreach (var property in keyProperties.Union(explicitKeyProperties))
                {
                    currentCount++;
                    adapter.AppendColumnNameEqualsValue(sb, property.Name); //fix for issue #336

                    if (currentCount < totalKeys)
                    {
                        sb.AppendFormat(" AND ");
                    }
                }

                return sb.ToString();
            });

            return sql;
        }

        /// <summary>
        /// Delete all entities in the table related to the type T.
        /// </summary>
        /// <typeparam name="T">Type of entity</typeparam>
        /// <param name="connection">Open SqlConnection</param>
        /// <param name="transaction">The transaction to run under, null (the default) if none</param>
        /// <param name="commandTimeout">Number of seconds before command execution timeout</param>
        /// <returns>true if deleted, false if none found</returns>
        public static bool DeleteAll<T>(this IDbConnection connection, IDbTransaction transaction = null,
            int? commandTimeout = null) where T : class
        {
            var type = typeof(T);
            var statement = "DELETE FROM " + GetTableName(type);
            var deleted = connection.Execute(statement, null, transaction, commandTimeout);
            return deleted > 0;
        }

        /// <summary>
        /// Specifies a custom callback that detects the database type instead of relying on the default strategy (the name of the connection type object).
        /// Please note that this callback is global and will be used by all the calls that require a database specific adapter.
        /// </summary>
        public static GetDatabaseTypeDelegate GetDatabaseType;

        private static ISqlAdapter GetFormatter(IDbConnection connection)
        {
            var name = GetDatabaseType?.Invoke(connection).ToLower()
                       ?? connection.GetType().Name.ToLower();

            return !AdapterDictionary.ContainsKey(name)
                ? DefaultAdapter
                : AdapterDictionary[name];
        }

        private static class ProxyGenerator
        {
            private static readonly Dictionary<Type, Type> TypeCache = new Dictionary<Type, Type>();

            private static AssemblyBuilder GetAsmBuilder(string name)
            {
#if NETSTANDARD1_3 || NETSTANDARD2_0
                return AssemblyBuilder.DefineDynamicAssembly(new AssemblyName { Name =
name }, AssemblyBuilderAccess.Run);
#else
                return Thread.GetDomain().DefineDynamicAssembly(new AssemblyName
                {
                    Name = name
                }, AssemblyBuilderAccess.Run);
#endif
            }

            public static T GetInterfaceProxy<T>()
            {
                Type typeOfT = typeof(T);

                if (TypeCache.TryGetValue(typeOfT, out Type k))
                {
                    return (T)Activator.CreateInstance(k);
                }
                var assemblyBuilder = GetAsmBuilder(typeOfT.Name);

                var moduleBuilder =
                    assemblyBuilder.DefineDynamicModule("SqlMapperExtensions." +
                                                        typeOfT.Name); //NOTE: to save, add "asdasd.dll" parameter

                var interfaceType = typeof(IProxy);
                var typeBuilder = moduleBuilder.DefineType(typeOfT.Name + "_" + Guid.NewGuid(),
                    TypeAttributes.Public | TypeAttributes.Class);
                typeBuilder.AddInterfaceImplementation(typeOfT);
                typeBuilder.AddInterfaceImplementation(interfaceType);

                //create our _isDirty field, which implements IProxy
                var setIsDirtyMethod = CreateIsDirtyProperty(typeBuilder);

                // Generate a field for each property, which implements the T
                foreach (var property in typeof(T).GetProperties())
                {
                    var isId = property.GetCustomAttributes(true).Any(a => a is KeyAttribute);
                    CreateProperty<T>(typeBuilder, property.Name, property.PropertyType, setIsDirtyMethod, isId);
                }

#if NETSTANDARD1_3 || NETSTANDARD2_0
                var generatedType = typeBuilder.CreateTypeInfo().AsType();
#else
                var generatedType = typeBuilder.CreateType();
#endif

                TypeCache.Add(typeOfT, generatedType);
                return (T)Activator.CreateInstance(generatedType);
            }

            private static MethodInfo CreateIsDirtyProperty(TypeBuilder typeBuilder)
            {
                var propType = typeof(bool);
                var field = typeBuilder.DefineField("_" + nameof(IProxy.IsDirty), propType, FieldAttributes.Private);
                var property = typeBuilder.DefineProperty(nameof(IProxy.IsDirty),
                    System.Reflection.PropertyAttributes.None,
                    propType,
                    new[] { propType });

                const MethodAttributes getSetAttr = MethodAttributes.Public | MethodAttributes.NewSlot |
                                                    MethodAttributes.SpecialName
                                                    | MethodAttributes.Final | MethodAttributes.Virtual |
                                                    MethodAttributes.HideBySig;

                // Define the "get" and "set" accessor methods
                var currGetPropMthdBldr = typeBuilder.DefineMethod("get_" + nameof(IProxy.IsDirty),
                    getSetAttr,
                    propType,
                    Type.EmptyTypes);
                var currGetIl = currGetPropMthdBldr.GetILGenerator();
                currGetIl.Emit(OpCodes.Ldarg_0);
                currGetIl.Emit(OpCodes.Ldfld, field);
                currGetIl.Emit(OpCodes.Ret);
                var currSetPropMthdBldr = typeBuilder.DefineMethod("set_" + nameof(IProxy.IsDirty),
                    getSetAttr,
                    null,
                    new[] { propType });
                var currSetIl = currSetPropMthdBldr.GetILGenerator();
                currSetIl.Emit(OpCodes.Ldarg_0);
                currSetIl.Emit(OpCodes.Ldarg_1);
                currSetIl.Emit(OpCodes.Stfld, field);
                currSetIl.Emit(OpCodes.Ret);

                property.SetGetMethod(currGetPropMthdBldr);
                property.SetSetMethod(currSetPropMthdBldr);
                var getMethod = typeof(IProxy).GetMethod("get_" + nameof(IProxy.IsDirty));
                var setMethod = typeof(IProxy).GetMethod("set_" + nameof(IProxy.IsDirty));
                typeBuilder.DefineMethodOverride(currGetPropMthdBldr, getMethod);
                typeBuilder.DefineMethodOverride(currSetPropMthdBldr, setMethod);

                return currSetPropMthdBldr;
            }

            private static void CreateProperty<T>(TypeBuilder typeBuilder, string propertyName, Type propType,
                MethodInfo setIsDirtyMethod, bool isIdentity)
            {
                //Define the field and the property 
                var field = typeBuilder.DefineField("_" + propertyName, propType, FieldAttributes.Private);
                var property = typeBuilder.DefineProperty(propertyName,
                    System.Reflection.PropertyAttributes.None,
                    propType,
                    new[] { propType });

                const MethodAttributes getSetAttr = MethodAttributes.Public
                                                    | MethodAttributes.Virtual
                                                    | MethodAttributes.HideBySig;

                // Define the "get" and "set" accessor methods
                var currGetPropMthdBldr = typeBuilder.DefineMethod("get_" + propertyName,
                    getSetAttr,
                    propType,
                    Type.EmptyTypes);

                var currGetIl = currGetPropMthdBldr.GetILGenerator();
                currGetIl.Emit(OpCodes.Ldarg_0);
                currGetIl.Emit(OpCodes.Ldfld, field);
                currGetIl.Emit(OpCodes.Ret);

                var currSetPropMthdBldr = typeBuilder.DefineMethod("set_" + propertyName,
                    getSetAttr,
                    null,
                    new[] { propType });

                //store value in private field and set the isdirty flag
                var currSetIl = currSetPropMthdBldr.GetILGenerator();
                currSetIl.Emit(OpCodes.Ldarg_0);
                currSetIl.Emit(OpCodes.Ldarg_1);
                currSetIl.Emit(OpCodes.Stfld, field);
                currSetIl.Emit(OpCodes.Ldarg_0);
                currSetIl.Emit(OpCodes.Ldc_I4_1);
                currSetIl.Emit(OpCodes.Call, setIsDirtyMethod);
                currSetIl.Emit(OpCodes.Ret);

                //TODO: Should copy all attributes defined by the interface?
                if (isIdentity)
                {
                    var keyAttribute = typeof(KeyAttribute);
                    var myConstructorInfo = keyAttribute.GetConstructor(new Type[] { });
                    var attributeBuilder = new CustomAttributeBuilder(myConstructorInfo, new object[] { });
                    property.SetCustomAttribute(attributeBuilder);
                }

                property.SetGetMethod(currGetPropMthdBldr);
                property.SetSetMethod(currSetPropMthdBldr);
                var getMethod = typeof(T).GetMethod("get_" + propertyName);
                var setMethod = typeof(T).GetMethod("set_" + propertyName);
                typeBuilder.DefineMethodOverride(currGetPropMthdBldr, getMethod);
                typeBuilder.DefineMethodOverride(currSetPropMthdBldr, setMethod);
            }
        }
    }
}
