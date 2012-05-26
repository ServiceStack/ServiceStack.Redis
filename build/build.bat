REM SET BUILD=Debug
SET BUILD=Release

COPY ..\src\ServiceStack.Redis\bin\%BUILD%\ServiceStack.Redis.* ..\NuGet\lib\net35

COPY ..\src\ServiceStack.Redis\bin\%BUILD%\ServiceStack.* ..\..\ServiceStack\release\latest\ServiceStack.Redis

COPY ..\src\ServiceStack.Redis\bin\%BUILD%\ServiceStack.* ..\..\ServiceStack\lib
COPY ..\src\ServiceStack.Redis\bin\%BUILD%\ServiceStack.* ..\..\ServiceStack.Examples\lib
COPY ..\src\ServiceStack.Redis\bin\%BUILD%\ServiceStack.Redis.* ..\..\ServiceStack.Contrib\lib
COPY ..\src\ServiceStack.Redis\bin\%BUILD%\ServiceStack.Redis.* ..\..\ServiceStack.RedisWebServices\lib


