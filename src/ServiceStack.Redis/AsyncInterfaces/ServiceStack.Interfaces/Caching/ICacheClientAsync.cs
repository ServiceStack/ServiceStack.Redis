using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace ServiceStack.Caching
{
    /// <summary>
    /// A common interface implementation that is implemented by most cache providers
    /// </summary>
    public interface ICacheClientAsync
        : IAsyncDisposable
    {
        /// <summary>
        /// Removes the specified item from the cache.
        /// </summary>
        /// <param name="key">The identifier for the item to delete.</param>
        /// <returns>
        /// true if the item was successfully removed from the cache; false otherwise.
        /// </returns>
        ValueTask<bool> RemoveAsync(string key, CancellationToken cancellationToken = default);

        /// <summary>
        /// Removes the cache for all the keys provided.
        /// </summary>
        /// <param name="keys">The keys.</param>
        ValueTask RemoveAllAsync(IEnumerable<string> keys, CancellationToken cancellationToken = default);

        /// <summary>
        /// Retrieves the specified item from the cache.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="key">The identifier for the item to retrieve.</param>
        /// <returns>
        /// The retrieved item, or <value>null</value> if the key was not found.
        /// </returns>
        ValueTask<T> GetAsync<T>(string key, CancellationToken cancellationToken = default);

        /// <summary>
        /// Increments the value of the specified key by the given amount.
        /// The operation is atomic and happens on the server.
        /// A non existent value at key starts at 0
        /// </summary>
        /// <param name="key">The identifier for the item to increment.</param>
        /// <param name="amount">The amount by which the client wants to increase the item.</param>
        /// <returns>
        /// The new value of the item or -1 if not found.
        /// </returns>
        /// <remarks>The item must be inserted into the cache before it can be changed. The item must be inserted as a <see cref="T:System.String"/>. The operation only works with <see cref="System.UInt32"/> values, so -1 always indicates that the item was not found.</remarks>
        ValueTask<long> IncrementAsync(string key, uint amount, CancellationToken cancellationToken = default);

        /// <summary>
        /// Increments the value of the specified key by the given amount.
        /// The operation is atomic and happens on the server.
        /// A non existent value at key starts at 0
        /// </summary>
        /// <param name="key">The identifier for the item to increment.</param>
        /// <param name="amount">The amount by which the client wants to decrease the item.</param>
        /// <returns>
        /// The new value of the item or -1 if not found.
        /// </returns>
        /// <remarks>The item must be inserted into the cache before it can be changed. The item must be inserted as a <see cref="T:System.String"/>. The operation only works with <see cref="System.UInt32"/> values, so -1 always indicates that the item was not found.</remarks>
        ValueTask<long> DecrementAsync(string key, uint amount, CancellationToken cancellationToken = default);

        /// <summary>
        /// Adds a new item into the cache at the specified cache key only if the cache is empty.
        /// </summary>
        /// <param name="key">The key used to reference the item.</param>
        /// <param name="value">The object to be inserted into the cache.</param>
        /// <returns>
        /// true if the item was successfully stored in the cache; false otherwise.
        /// </returns>
        /// <remarks>The item does not expire unless it is removed due memory pressure.</remarks>
        ValueTask<bool> AddAsync<T>(string key, T value, CancellationToken cancellationToken = default);

        /// <summary>
        /// Sets an item into the cache at the cache key specified regardless if it already exists or not.
        /// </summary>
        ValueTask<bool> SetAsync<T>(string key, T value, CancellationToken cancellationToken = default);

        /// <summary>
        /// Replaces the item at the cachekey specified only if an items exists at the location already.
        /// </summary>
        ValueTask<bool> ReplaceAsync<T>(string key, T value, CancellationToken cancellationToken = default);

        ValueTask<bool> AddAsync<T>(string key, T value, DateTime expiresAt, CancellationToken cancellationToken = default);
        ValueTask<bool> SetAsync<T>(string key, T value, DateTime expiresAt, CancellationToken cancellationToken = default);
        ValueTask<bool> ReplaceAsync<T>(string key, T value, DateTime expiresAt, CancellationToken cancellationToken = default);

        ValueTask<bool> AddAsync<T>(string key, T value, TimeSpan expiresIn, CancellationToken cancellationToken = default);
        ValueTask<bool> SetAsync<T>(string key, T value, TimeSpan expiresIn, CancellationToken cancellationToken = default);
        ValueTask<bool> ReplaceAsync<T>(string key, T value, TimeSpan expiresIn, CancellationToken cancellationToken = default);

        /// <summary>
        /// Invalidates all data on the cache.
        /// </summary>
        ValueTask FlushAllAsync(CancellationToken cancellationToken = default);

        /// <summary>
        /// Retrieves multiple items from the cache. 
        /// The default value of T is set for all keys that do not exist.
        /// </summary>
        /// <param name="keys">The list of identifiers for the items to retrieve.</param>
        /// <returns>
        /// a Dictionary holding all items indexed by their key.
        /// </returns>
        ValueTask<IDictionary<string, T>> GetAllAsync<T>(IEnumerable<string> keys, CancellationToken cancellationToken = default);

        /// <summary>
        /// Sets multiple items to the cache. 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="values">The values.</param>
        ValueTask SetAllAsync<T>(IDictionary<string, T> values, CancellationToken cancellationToken = default);
    }
}