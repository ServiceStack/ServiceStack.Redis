REM SET BUILD=Debug
SET BUILD=Release

COPY ..\src\ServiceStack.Redis\bin\%BUILD%\ServiceStack.Redis.* ..\NuGet\lib\net45

COPY ..\src\ServiceStack.Redis\bin\%BUILD%\ServiceStack.Redis.* ..\..\ServiceStack\lib
COPY ..\src\ServiceStack.Redis\bin\Signed\ServiceStack.Redis.* ..\..\ServiceStack\lib\signed
