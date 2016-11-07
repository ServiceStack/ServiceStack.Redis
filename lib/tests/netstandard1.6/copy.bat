SET BUILD=Debug
REM SET BUILD=Release

COPY ..\..\..\ServiceStack\src\ServiceStack\bin\%BUILD%\netstandard1.6\ServiceStack.dll .
COPY ..\..\..\ServiceStack\src\ServiceStack\bin\%BUILD%\netstandard1.6\ServiceStack.pdb .
COPY ..\..\..\ServiceStack\src\ServiceStack\bin\%BUILD%\netstandard1.6\ServiceStack.deps.json .

