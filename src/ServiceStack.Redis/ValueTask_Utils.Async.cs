using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;

namespace ServiceStack.Redis.Internal
{
    internal static class ValueTask_Utils
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static ValueTask Await<T>(this ValueTask<T> pending)
        {
            if (pending.IsCompletedSuccessfully)
            {
                _ = pending.Result; // for IValueTaskSource reasons
                return default;
            }
            else
            {
                return Awaited(pending);
            }
            async static ValueTask Awaited(ValueTask<T> pending)
                => await pending.ConfigureAwait(false);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static ValueTask<TTo> Await<TFrom, TTo>(this ValueTask<TFrom> pending, Func<TFrom, TTo> projection)
        {
            return pending.IsCompletedSuccessfully ? projection(pending.Result).AsValueTask() : Awaited(pending, projection);
            async static ValueTask<TTo> Awaited(ValueTask<TFrom> pending, Func<TFrom, TTo> projection)
                => projection(await pending.ConfigureAwait(false));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static ValueTask<int> AsInt32(this ValueTask<long> pending)
        {
            return pending.IsCompletedSuccessfully ? (checked((int)pending.Result)).AsValueTask() : Awaited(pending);
            async static ValueTask<int> Awaited(ValueTask<long> pending)
                => checked((int)await pending.ConfigureAwait(false));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static ValueTask<TTo> Await<TFrom, TTo, TState>(this ValueTask<TFrom> pending, Func<TFrom, TState, TTo> projection, TState state)
        {
            return pending.IsCompletedSuccessfully ? projection(pending.Result, state).AsValueTask() : Awaited(pending, projection, state);
            async static ValueTask<TTo> Awaited(ValueTask<TFrom> pending, Func<TFrom, TState, TTo> projection, TState state)
                => projection(await pending.ConfigureAwait(false), state);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static ValueTask<bool> AwaitAsTrue(this ValueTask pending)
        {
            if (pending.IsCompletedSuccessfully)
            {
                pending.GetAwaiter().GetResult(); // for IValueTaskSource reasons
                return s_ValueTaskTrue;
            }
            else
            {
                return Awaited(pending);
            }
            async static ValueTask<bool> Awaited(ValueTask pending)
            {
                await pending.ConfigureAwait(false);
                return true;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static ValueTask<bool> AwaitAsTrue<T>(this ValueTask<T> pending)
        {
            if (pending.IsCompletedSuccessfully)
            {
                _ = pending.Result; // for IValueTaskSource reasons
                return s_ValueTaskTrue;
            }
            else
            {
                return Awaited(pending);
            }
            async static ValueTask<bool> Awaited(ValueTask<T> pending)
            {
                await pending.ConfigureAwait(false);
                return true;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static ValueTask<bool> IsSuccessAsync(this ValueTask<long> pending)
        {
            return pending.IsCompletedSuccessfully ? (pending.Result == RedisNativeClient.Success).AsValueTask() : Awaited(pending);
            async static ValueTask<bool> Awaited(ValueTask<long> pending)
                => (await pending.ConfigureAwait(false)) == RedisNativeClient.Success;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static ValueTask<List<TValue>> ConvertEachToAsync<TValue>(this ValueTask<List<string>> pending)
        {
            return pending.IsCompletedSuccessfully ? pending.Result.ConvertEachTo<TValue>().AsValueTask() : Awaited(pending);
            static async ValueTask<List<TValue>> Awaited(ValueTask<List<string>> pending)
                => (await pending.ConfigureAwait(false)).ConvertEachTo<TValue>();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static ValueTask<List<string>> ToStringListAsync(this ValueTask<byte[][]> pending)
        {
            return pending.IsCompletedSuccessfully ? pending.Result.ToStringList().AsValueTask() : Awaited(pending);
            static async ValueTask<List<string>> Awaited(ValueTask<byte[][]> pending)
                => (await pending.ConfigureAwait(false)).ToStringList();
        }

        private static readonly ValueTask<bool> s_ValueTaskTrue = true.AsValueTask();

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static ValueTask<T> Await<T>(this ValueTask pending, T result)
        {
            return pending.IsCompletedSuccessfully ? result.AsValueTask() : Awaited(pending, result);
            async static ValueTask<T> Awaited(ValueTask pending, T result)
            {
                await pending.ConfigureAwait(false);
                return result;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static ValueTask<T> AsValueTask<T>(this T value) => new ValueTask<T>(value);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static ValueTask<string> FromUtf8BytesAsync(this ValueTask<byte[]> pending)
        {
            return pending.IsCompletedSuccessfully ? pending.Result.FromUtf8Bytes().AsValueTask() : Awaited(pending);
            static async ValueTask<string> Awaited(ValueTask<byte[]> pending)
                => (await pending.ConfigureAwait(false)).FromUtf8Bytes();
        }
    }
}
