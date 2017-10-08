REM SET BUILD=Debug
SET BUILD=Release

COPY ..\src\ServiceStack.Redis\bin\%BUILD%\net45\ServiceStack.Redis.* ..\..\ServiceStack\lib\net45
COPY ..\src\ServiceStack.Redis\bin\%BUILD%\netstandard2.0\ServiceStack.Redis.* ..\..\ServiceStack\lib\netstandard2.0
COPY ..\src\ServiceStack.Redis\bin\Signed\net45\ServiceStack.Redis.* ..\..\ServiceStack\lib\signed
