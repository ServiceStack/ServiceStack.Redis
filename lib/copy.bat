REM SET BUILD=Debug
SET BUILD=Release

COPY "..\..\ServiceStack\src\ServiceStack.Interfaces\bin\%BUILD%\portable40-net45+sl5+win8+wp8+wpa81\ServiceStack.Interfaces.*" pcl

COPY ..\..\ServiceStack.Text\src\ServiceStack.Text\bin\%BUILD%\net45\ServiceStack.Text.* net45
COPY ..\..\ServiceStack.Text\src\ServiceStack.Text\bin\%BUILD%\netstandard1.1\ServiceStack.Text.* netstandard1.1
COPY ..\..\ServiceStack.Text\src\ServiceStack.Text\bin\%BUILD%\netstandard1.3\ServiceStack.Text.* netstandard1.3
COPY ..\..\ServiceStack.Text\src\ServiceStack.Text\bin\Signed\net45\ServiceStack.Text.* signed

COPY ..\..\ServiceStack\src\ServiceStack.Client\bin\%BUILD%\net45\ServiceStack.Client.* net45
COPY ..\..\ServiceStack\src\ServiceStack.Client\bin\%BUILD%\netstandard1.1\ServiceStack.Client.* netstandard1.1
COPY ..\..\ServiceStack\src\ServiceStack.Client\bin\%BUILD%\netstandard1.6\ServiceStack.Client.* netstandard1.6
COPY ..\..\ServiceStack\src\ServiceStack.Client\bin\Signed\net45\ServiceStack.Client.* signed

COPY ..\..\ServiceStack\src\ServiceStack.Common\bin\%BUILD%\net45\ServiceStack.Common.* net45
COPY ..\..\ServiceStack\src\ServiceStack.Common\bin\%BUILD%\netstandard1.3\ServiceStack.Common.* netstandard1.3

COPY ..\..\ServiceStack\src\ServiceStack\bin\%BUILD%\net45\ServiceStack.dll net45
COPY ..\..\ServiceStack\src\ServiceStack\bin\%BUILD%\net45\ServiceStack.xml net45
COPY ..\..\ServiceStack\src\ServiceStack\bin\%BUILD%\netstandard1.6\ServiceStack.dll netstandard1.6
COPY ..\..\ServiceStack\src\ServiceStack\bin\%BUILD%\netstandard1.6\ServiceStack.xml netstandard1.6
COPY ..\..\ServiceStack\src\ServiceStack\bin\Signed\net45\ServiceStack.dll signed
COPY ..\..\ServiceStack\src\ServiceStack\bin\Signed\net45\ServiceStack.xml signed

COPY ..\..\ServiceStack\src\ServiceStack.Server\bin\%BUILD%\net45\ServiceStack.Server.* net45
COPY ..\..\ServiceStack\src\ServiceStack.Server\bin\%BUILD%\netstandard1.6\ServiceStack.Server.* netstandard1.6
