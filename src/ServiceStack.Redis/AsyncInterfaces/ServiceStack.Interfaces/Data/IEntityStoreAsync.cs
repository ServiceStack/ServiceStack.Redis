//Copyright (c) ServiceStack, Inc. All Rights Reserved.
//License: https://raw.github.com/ServiceStack/ServiceStack/master/license.txt

using System;
using System.Collections;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace ServiceStack.Data
{
    public interface IEntityStoreAsync : IAsyncDisposable
    {
        ValueTask<T> GetByIdAsync<T>(object id, CancellationToken cancellationToken = default);

        ValueTask<IList<T>> GetByIdsAsync<T>(ICollection ids, CancellationToken cancellationToken = default);

        ValueTask<T> StoreAsync<T>(T entity, CancellationToken cancellationToken = default);

        ValueTask StoreAllAsync<TEntity>(IEnumerable<TEntity> entities, CancellationToken cancellationToken = default);

        ValueTask DeleteAsync<T>(T entity, CancellationToken cancellationToken = default);

        ValueTask DeleteByIdAsync<T>(object id, CancellationToken cancellationToken = default);

        ValueTask DeleteByIdsAsync<T>(ICollection ids, CancellationToken cancellationToken = default);

        ValueTask DeleteAllAsync<TEntity>(CancellationToken cancellationToken = default);
    }
}