@echo off

set target=%1
if "%target%" == "" (
   set target=UnitTestsWithoutIntegration
)

%WINDIR%\Microsoft.NET\Framework\v4.0.30319\msbuild Build\Build.proj /target:%target% /v:M /fl /flp:LogFile=msbuild.log;Verbosity=Normal /nr:false