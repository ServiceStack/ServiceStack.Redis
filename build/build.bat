SET MSBUILD="C:\Program Files (x86)\Microsoft Visual Studio\2017\Community\MSBuild\15.0\Bin\MSBuild.exe"

REM %MSBUILD% build-core.proj /target:TeamCityBuild;NuGetPack /property:Configuration=Release;PatchVersion=41
REM %MSBUILD% build.proj /target:TeamCityBuild;NuGetPack /property:Configuration=Release;PatchVersion=9
REM %MSBUILD% build-sn.proj /target:NuGetPack /property:Configuration=Signed;PatchVersion=9

msbuild /p:Configuration=Release ..\src\ServiceStack.Redis.sln
