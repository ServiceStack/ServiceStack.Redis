REM SET BUILD=Debug
SET BUILD=Release

COPY ..\src\ServiceStack.Redis\bin\%BUILD%\net45\ServiceStack.Redis.* ..\..\ServiceStack\lib
COPY ..\src\ServiceStack.Redis\bin\%BUILD%\netstandard1.3\ServiceStack.Redis.* ..\..\ServiceStack\lib\netstandard1.3
COPY ..\src\ServiceStack.Redis\bin\Signed\net45\ServiceStack.Redis.* ..\..\ServiceStack\lib\signed
