@echo off
set path=%PATH%;C:\Program Files\7-Zip;

rd .\Publish /q /s

md Publish

dotnet publish .\Plasma -c Release -o publish\Plasma --runtime win-x64 --self-contained false
del publish\Plasma\*.pdb
7z.exe a -tzip -r -mx9 ".\Plasma.zip" ".\Publish\Plasma\*.*"
rd .\Publish /q /s

pause
