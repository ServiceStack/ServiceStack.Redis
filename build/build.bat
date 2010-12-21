SET DEPLOY_PATH=C:\src\ServiceStack\release\latest\ServiceStack.Redis

REM SET BUILD=Debug
SET BUILD=Release

SET PROJ_LIBS=
SET PROJ_LIBS=%PROJ_LIBS% ..\lib\ServiceStack.Interfaces.dll
SET PROJ_LIBS=%PROJ_LIBS% ..\lib\ServiceStack.Client.dll
SET PROJ_LIBS=%PROJ_LIBS% ..\lib\ServiceStack.Common.dll
SET PROJ_LIBS=%PROJ_LIBS% ..\lib\ServiceStack.Text.dll
SET PROJ_LIBS=%PROJ_LIBS% ..\lib\ServiceStack.Messaging.dll
SET PROJ_LIBS=%PROJ_LIBS% ..\src\ServiceStack.Redis\bin\%BUILD%\ServiceStack.Redis.dll

ILMerge.exe /ndebug /t:library /out:ServiceStack.Redis.dll %PROJ_LIBS%
COPY *.dll %DEPLOY_PATH%
COPY ..\src\ServiceStack.Redis\bin\%BUILD%\ServiceStack.Redis.dll ..\..\ServiceStack\release\lib
COPY ..\src\ServiceStack.Redis\bin\%BUILD%\ServiceStack.Redis.pdb ..\..\ServiceStack\release\lib

COPY ..\src\ServiceStack.Redis\bin\%BUILD%\ServiceStack.Redis.dll ..\..\ServiceStack\lib
COPY ..\src\ServiceStack.Redis\bin\%BUILD%\ServiceStack.Redis.pdb ..\..\ServiceStack\lib
