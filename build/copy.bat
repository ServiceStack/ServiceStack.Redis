REM SET BUILD=Debug
SET BUILD=Release

COPY ..\src\ServiceStack.Redis\bin\%BUILD%\ServiceStack.Redis.* ..\NuGet\lib\net40

COPY ..\src\ServiceStack.Redis\bin\%BUILD%\ServiceStack.Redis.* ..\..\ServiceStack\lib
COPY ..\src\ServiceStack.Redis\bin\%BUILD%\ServiceStack.Redis.* ..\..\ServiceStack.Contrib\lib
COPY ..\src\ServiceStack.Redis\bin\%BUILD%\ServiceStack.Redis.* ..\..\ServiceStack.RedisWebServices\lib

COPY ..\src\ServiceStack.Redis\bin\%BUILD%\ServiceStack.Redis.* ..\..\ServiceStack\lib\signed
