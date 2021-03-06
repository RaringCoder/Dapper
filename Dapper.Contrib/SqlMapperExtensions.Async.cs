﻿using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace Dapper.Contrib.Extensions
{
    public static partial class SqlMapperExtensions
    {
        /// <summary>
        /// Returns a single entity by a single id from table "Ts" asynchronously using .NET 4.5 Task. T must be of interface type. 
        /// Id must be marked with [Key] attribute.
        /// Created entity is tracked/intercepted for changes and used by the Update() extension. 
        /// </summary>
        /// <typeparam name="T">Interface type to create and populate</typeparam>
        /// <param name="connection">Open SqlConnection</param>
        /// <param name="id">Id of the entity to get, must be marked with [Key] attribute</param>
        /// <param name="transaction">The transaction to run under, null (the default) if none</param>
        /// <param name="commandTimeout">Number of seconds before command execution timeout</param>
        /// <returns>Entity of T</returns>
        public static async Task<T> GetAsync<T>(
            this IDbConnection connection, 
            dynamic id,
            IDbTransaction transaction = null, 
            int? commandTimeout = null) where T : class
        {
            var type = typeof(T);

            var sql = GetSelectScript<T>(type, nameof(GetAsync));

            var dynParms = new DynamicParameters();
            dynParms.Add("@id", id);

            if (!type.IsInterface())
            {
                var results = await connection
                    .QueryAsync<T>(sql, dynParms, transaction, commandTimeout)
                    .ConfigureAwait(false);

                return results.FirstOrDefault();
            }
            
            return MapResultToType<T>(
                await connection.QueryAsync<dynamic>(sql, dynParms, transaction, commandTimeout).ConfigureAwait(false),
                type);
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
        public static Task<IEnumerable<T>> GetAllAsync<T>(this IDbConnection connection,
            IDbTransaction transaction = null, int? commandTimeout = null) where T : class
        {
            var type = typeof(T);
            var sql = GetSelectAllScript<T>(nameof(GetAllAsync));

            if (!type.IsInterface())
            {
                return connection.QueryAsync<T>(sql, null, transaction, commandTimeout);
            }

            return GetAllAsyncImpl<T>(connection, transaction, commandTimeout, sql, type);
        }

        private static async Task<IEnumerable<T>> GetAllAsyncImpl<T>(IDbConnection connection,
            IDbTransaction transaction, int? commandTimeout, string sql, Type type) where T : class
        {
            var result = await connection.QueryAsync(
                    sql, 
                    transaction: transaction, 
                    commandTimeout: commandTimeout)
                .ConfigureAwait(false);

            return MapResultsToType<T>(result, type);
        }

        /// <summary>
        /// Inserts an entity into table "Ts" asynchronously using .NET 4.5 Task and returns identity id.
        /// </summary>
        /// <typeparam name="T">The type being inserted.</typeparam>
        /// <param name="connection">Open SqlConnection</param>
        /// <param name="entityToInsert">Entity to insert</param>
        /// <param name="transaction">The transaction to run under, null (the default) if none</param>
        /// <param name="commandTimeout">Number of seconds before command execution timeout</param>
        /// <param name="sqlAdapter">The specific ISqlAdapter to use, auto-detected based on connection if null</param>
        /// <returns>Identity of inserted entity</returns>
        public static Task<int> InsertAsync<T>(
            this IDbConnection connection, 
            T entityToInsert,
            IDbTransaction transaction = null,
            int? commandTimeout = null, 
            ISqlAdapter sqlAdapter = null) where T : class
        {
            if (entityToInsert == null)
            {
                throw new ArgumentNullException(nameof(entityToInsert), "Cannot insert a null object.");
            }
            
            sqlAdapter = sqlAdapter ?? GetFormatter(connection);

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
            var allPropertiesExceptKeyComputedAndRowVersion =
                allProperties.Except(keyProperties.Union(computedProperties).Union(OptionalRowVersion(rowVersion))).ToList();

            var sbColumnList = new StringBuilder();
            var sbParameterList = new StringBuilder();

            for (var i = 0; i < allPropertiesExceptKeyComputedAndRowVersion.Count; i++)
            {
                if (i != 0)
                {
                    sbColumnList.Append(", ");
                    sbParameterList.Append(", ");
                }
                
                sqlAdapter.AppendColumnName(sbColumnList, allPropertiesExceptKeyComputedAndRowVersion[i].Name);
                sbParameterList.AppendFormat("@{0}", allPropertiesExceptKeyComputedAndRowVersion[i].Name);
            }

            if (!isList) //single entity
            {
                return sqlAdapter.InsertAsync(connection, transaction, commandTimeout, name, sbColumnList.ToString(),
                    sbParameterList.ToString(), keyProperties, entityToInsert);
            }

            //insert list of entities
            var cmd = $"INSERT INTO {name} ({sbColumnList}) values ({sbParameterList})";

            return connection.ExecuteAsync(cmd, entityToInsert, transaction, commandTimeout);
        }

        /// <summary>
        /// Updates entity in table "Ts" asynchronously using .NET 4.5 Task, checks if the entity is modified if the entity is tracked by the Get() extension.
        /// </summary>
        /// <typeparam name="T">Type to be updated</typeparam>
        /// <param name="connection">Open SqlConnection</param>
        /// <param name="entityToUpdate">Entity to be updated</param>
        /// <param name="transaction">The transaction to run under, null (the default) if none</param>
        /// <param name="commandTimeout">Number of seconds before command execution timeout</param>
        /// <returns>true if updated, false if not found or not modified (tracked entities)</returns>
        public static async Task<bool> UpdateAsync<T>(
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

            var updated = await connection.ExecuteAsync(
                    updateScript, 
                    entityToUpdate, 
                    commandTimeout: commandTimeout, 
                    transaction: transaction)
                .ConfigureAwait(false);

            return updated > 0;
        }

        /// <summary>
        /// Delete entity in table "Ts" asynchronously using .NET 4.5 Task.
        /// </summary>
        /// <typeparam name="T">Type of entity</typeparam>
        /// <param name="connection">Open SqlConnection</param>
        /// <param name="entityToDelete">Entity to delete</param>
        /// <param name="transaction">The transaction to run under, null (the default) if none</param>
        /// <param name="commandTimeout">Number of seconds before command execution timeout</param>
        /// <returns>true if deleted, false if not found</returns>
        public static async Task<bool> DeleteAsync<T>(
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

            var deleted = await connection.ExecuteAsync(
                    deleteScript, 
                    entityToDelete, 
                    transaction, 
                    commandTimeout)
                .ConfigureAwait(false);

            return deleted > 0;
        }

        /// <summary>
        /// Delete all entities in the table related to the type T asynchronously using .NET 4.5 Task.
        /// </summary>
        /// <typeparam name="T">Type of entity</typeparam>
        /// <param name="connection">Open SqlConnection</param>
        /// <param name="transaction">The transaction to run under, null (the default) if none</param>
        /// <param name="commandTimeout">Number of seconds before command execution timeout</param>
        /// <returns>true if deleted, false if none found</returns>
        public static async Task<bool> DeleteAllAsync<T>(this IDbConnection connection,
            IDbTransaction transaction = null, int? commandTimeout = null) where T : class
        {
            var type = typeof(T);
            var statement = "DELETE FROM " + GetTableName(type);
            var deleted = await connection.ExecuteAsync(statement, null, transaction, commandTimeout)
                .ConfigureAwait(false);
            return deleted > 0;
        }
    }
}
