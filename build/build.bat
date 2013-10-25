SET MSBUILD=C:\Windows\Microsoft.NET\Framework\v4.0.30319\msbuild.exe

REM %MSBUILD% build.proj /target:NuGetPack /property:Configuration=Release;RELEASE=true;PatchVersion=1
%MSBUILD% build.proj /target:NuGetPack /property:Configuration=Signed;RELEASE=true;PatchVersion=0
