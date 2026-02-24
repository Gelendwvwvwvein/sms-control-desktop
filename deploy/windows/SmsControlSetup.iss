[Setup]
AppId={{E2DB80E0-0E56-4A07-9A4E-53C05F60543A}
AppName=SMS Control
AppVersion=1.0.0
AppPublisher=SMS Control
DefaultDirName={localappdata}\Programs\SmsControl
DefaultGroupName=SMS Control
DisableProgramGroupPage=yes
OutputDir=..\..\out\installer
OutputBaseFilename=SmsControlSetup
Compression=lzma
SolidCompression=yes
WizardStyle=modern
PrivilegesRequired=lowest
UninstallDisplayIcon={app}\Collector.exe

[Languages]
Name: "russian"; MessagesFile: "compiler:Languages\Russian.isl"

[Files]
Source: "..\..\out\publish\win-x64\*"; DestDir: "{app}"; Flags: ignoreversion recursesubdirs createallsubdirs
Source: "SmsControlLauncher.ps1"; DestDir: "{app}"; Flags: ignoreversion

[Icons]
Name: "{autodesktop}\SMS Control"; Filename: "powershell.exe"; Parameters: "-NoProfile -ExecutionPolicy Bypass -File ""{app}\SmsControlLauncher.ps1"""; WorkingDir: "{app}"
Name: "{group}\SMS Control"; Filename: "powershell.exe"; Parameters: "-NoProfile -ExecutionPolicy Bypass -File ""{app}\SmsControlLauncher.ps1"""; WorkingDir: "{app}"
Name: "{group}\Удалить SMS Control"; Filename: "{uninstallexe}"

[Run]
Filename: "{app}\Collector.exe"; Parameters: "--db-migrate"; Description: "Инициализация базы данных"; Flags: runhidden waituntilterminated skipifsilent
Filename: "powershell.exe"; Parameters: "-ExecutionPolicy Bypass -File ""{app}\playwright.ps1"" install chromium"; Description: "Установка Playwright Chromium"; Flags: runhidden waituntilterminated skipifsilent; Check: FileExists(ExpandConstant('{app}\playwright.ps1'))
Filename: "powershell.exe"; Parameters: "-NoProfile -ExecutionPolicy Bypass -File ""{app}\SmsControlLauncher.ps1"""; Description: "Запустить SMS Control"; Flags: nowait postinstall skipifsilent
