//Copyright (c) ServiceStack, Inc. All Rights Reserved.
//License: https://raw.github.com/ServiceStack/ServiceStack/master/license.txt

using System;
using System.Collections;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace ServiceStack.Data
{
    /// <summary>
    /// For providers that want a cleaner API with a little more perf
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public interface IEntityStoreAsync<T>
    {
        ValueTask<T> GetByIdAsync(object id, CancellationToken cancellationToken = default);

        ValueTask<IList<T>> GetByIdsAsync(IEnumerable ids, CancellationToken cancellationToken = default);

        ValueTask<IList<T>> GetAllAsync(CancellationToken cancellationToken = default);

        ValueTask<T> StoreAsync(T entity, CancellationToken cancellationToken = default);

        ValueTask StoreAllAsync(IEnumerable<T> entities, CancellationToken cancellationToken = default);

        ValueTask DeleteAsync(T entity, CancellationToken cancellationToken = default);

        ValueTask DeleteByIdAsync(object id, CancellationToken cancellationToken = default);

        ValueTask DeleteByIdsAsync(IEnumerable ids, CancellationToken cancellationToken = default);

        ValueTask DeleteAllAsync(CancellationToken cancellationToken = default);
    }
}