REM SET BUILD=Debug
SET BUILD=Release

COPY ..\src\ServiceStack.Redis\bin\%BUILD%\ServiceStack.Redis.* ..\..\ServiceStack\release\latest
COPY ..\src\ServiceStack.Redis\bin\%BUILD%\ServiceStack.Redis.* ..\..\ServiceStack\release\latest\ServiceStack.Redis

COPY ..\src\ServiceStack.Redis\bin\%BUILD%\ServiceStack.Redis.* ..\..\ServiceStack\lib\tests
COPY ..\src\ServiceStack.Redis\bin\%BUILD%\ServiceStack.Redis.* ..\..\ServiceStack.Examples\lib
COPY ..\src\ServiceStack.Redis\bin\%BUILD%\ServiceStack.Redis.* ..\..\ServiceStack.Contrib\lib
COPY ..\src\ServiceStack.Redis\bin\%BUILD%\ServiceStack.Redis.* ..\..\ServiceStack.RedisWebServices\lib

