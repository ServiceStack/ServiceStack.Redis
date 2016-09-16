SET BUILD=Debug
REM SET BUILD=Release

COPY ..\..\..\ServiceStack.Text\src\ServiceStack.Text\bin\%BUILD%\netstandard1.1\ServiceStack.Text.* .\
COPY ..\..\..\ServiceStack.Text\src\ServiceStack.Text\bin\%BUILD%\netstandard1.3\ServiceStack.Text.* ..\netstandard1.3\
COPY ..\..\..\ServiceStack\src\ServiceStack.Common\bin\%BUILD%\netstandard1.3\ServiceStack.Common.* ..\netstandard1.3
COPY ..\..\..\ServiceStack\src\ServiceStack.Interfaces\bin\%BUILD%\netstandard1.1\ServiceStack.Interfaces.* .\

