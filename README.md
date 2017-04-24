Содрано с https://github.com/danielwertheim/mycouch. Переделано под Core.


Публикация пакета:

msbuild /t:pack /p:Configuration=Release

nuget push couch_client.backend.1.0.0.nupkg -Source http://cm-ylng-msk-04/nuget/nuget