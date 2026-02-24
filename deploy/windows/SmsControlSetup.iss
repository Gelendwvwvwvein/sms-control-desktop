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

[Icons]
Name: "{autodesktop}\SMS Control"; Filename: "{app}\Collector.exe"; Parameters: "--desktop --port 5057"; WorkingDir: "{app}"; IconFilename: "{app}\Collector.exe"
Name: "{group}\SMS Control"; Filename: "{app}\Collector.exe"; Parameters: "--desktop --port 5057"; WorkingDir: "{app}"; IconFilename: "{app}\Collector.exe"
Name: "{group}\Удалить SMS Control"; Filename: "{uninstallexe}"

[Run]
Filename: "{app}\Collector.exe"; Parameters: "--desktop --port 5057"; Description: "Запустить SMS Control"; Flags: nowait postinstall skipifsilent
