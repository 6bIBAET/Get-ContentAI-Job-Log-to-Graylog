# Скрипт для извлечения логов работы Content AI сервера в Graylog.
# Для использования необходимо: 
# 1. Powershell Graylog PSGELF модуль https://www.powershellgallery.com/packages/PSGELF/1.12
# 2. Sqlite библиотеки https://www.sqlite.org/download.html

Import-Module PSGELF

# Рабочие переменные которые необходимо заполнить
$hostName = "ocr-01"
$workFolder = "d:\tmp"
$pathToDb = "D:\JobsLogTable.db"
$pathToArchiveDbFolder = "D:\tmp\Archive"
$pathToSqlite = "D:\Script\System.Data.SQLite.dll"
$graylogServer = "logsrv-02"
$graylogServerPort = "5555"
#------------------------------------------------------------------
# Переменные работы скрипта
$ScriptLog = "$($workFolder)\OCRScripData\ocrlog.txt"
$retentionLog= "$($workFolder)\OCRScripData\ocrlog-1.txt"
$tableLastRow = "$($workFolder)\OCRScripData\lastrow.txt"
$dateFormat = "dd/MM/yyyy HH:mm:ss"
$jobTableName = "JOBMESSAGES"


# Загружаем библиотеку sqlite
[Reflection.Assembly]::LoadFile($pathToSqlite)

# Функция даты для лога скрипта
function CurrantDate {
    Get-Date -Format $dateFormat 
}

# Функция проверки номера последней записи 
function GetLastRow {
    try {
        $connection = New-Object System.Data.SQLite.SQLiteConnection
        $connection.ConnectionString = "Data Source=$pathToDb;Version=3;"
        $connection.open()
        Add-Content -Path $scriptLog -Value "$(CurrantDate) | Info | GetLastRow - Соединение с базой $($FileName) открыто"
        $query = "SELECT COUNT(*) FROM $jobTableName"
        $command = $connection.CreateCommand()
        $command.CommandText = $query
        [int]$rowCount = $command.ExecuteScalar()
        $connection.Close()
        Add-Content -Path $scriptLog -Value "$(CurrantDate) | Info | Строк в БД: $($rowCount)"
        Add-Content -Path $scriptLog -Value "$(CurrantDate) | Info | GetLastRow - Соединение с базой $($FileName) закрыто"
        return $rowCount
    }
    catch {
        $connection.Close()
        Add-Content -Path $scriptLog -Value "$(CurrantDate) | Error | Получение последней строки: $($Error[0])"
    }
}

# Функция получения последней проверенной строки
function LastCheckedRow {
    try{
        $fileContent = Get-Content -Path $tableLastRow -Raw
        $numberFromFile = [int]$fileContent
        Add-Content -Path $scriptLog -Value "$(CurrantDate) | Info | Последняя проверенная строка: $($numberFromFile)"
        return $numberFromFile
    }
    catch{
        Add-Content -Path $scriptLog -Value "$(CurrantDate) | Error | Ошибка получения последней проверенной строки: $($Error[0])"
        break
    }
}

# Функция определения свежей архивной копии 
function GetArchiveDbFileName {
    try{
        $firstNewFile = Get-ChildItem -Path $pathToArchiveDbFolder | Sort-Object LastWriteTime | Select-Object -First 1
        Add-Content -Path $scriptLog -Value "$(CurrantDate) | Info | Свежий файл архива: $($firstNewFile)"
        return $firstNewFile
    }
    catch{
        Add-Content -Path $scriptLog -Value "$(CurrantDate) | Error | Ошибка определения свежего архивного файла, возможно нет ни одного файла по пути $($pathToArchiveDbFolder). Текст ошибки: $($Error[0])"
        break
    }
}

# Функция запроса данных из нужной БД и запись на Graylog
function GetDataAndWriteToGraylog {
    param (
        [string]$FileName,
        [int]$Row
    )
    $array = @()
    try {
        $connection = New-Object System.Data.SQLite.SQLiteConnection
        $connection.ConnectionString = "Data Source=$FileName;Version=3;"
        $connection.open()
        Add-Content -Path $scriptLog -Value "$(CurrantDate) | Info | Соединение с базой $($FileName) открыто"
        $query = "SELECT * FROM [$JobTableName] ORDER BY ROWID LIMIT -1 OFFSET $($Row);"
        $command = $connection.CreateCommand()
        $command.CommandText = $query
        $dataReader = $command.ExecuteReader()
        Add-Content -Path $scriptLog -Value "$(CurrantDate) | Info | Ридер открыт"
    }
    catch{
        Add-Content -Path $scriptLog -Value "$(CurrantDate) | Error | Создание соединения с базой $($FileName): $($Error[0])"
        break
    }

    # Заполняем массив данными
    try {
        while ($dataReader.Read()) {
            $array += "$($dataReader["XML"].replace('<Params>','"').replace('</Params>','"').replace('</Message>','"').split("`n")|%{
                    $_.split('"')[1,3,7] -join " | "
                }) | $($dataReader["SEARCHABLETEXT"])"
        
        }
        Add-Content -Path $scriptLog -Value "$(CurrantDate) | Info | Прочитано элементов: $(@($array).Count)"
        $dataReader.Close()
        Add-Content -Path $scriptLog -Value "$(CurrantDate) | Info | Ридер закрыт"
        $connection.Close()
        Add-Content -Path $scriptLog -Value "$(CurrantDate) | Info | Соединение с базой $($FileName) закрыто"
        
    }
    catch{
        $dataReader.Close()
        $connection.Close()
        Add-Content -Path $scriptLog -Value "$(CurrantDate) | Error | Получение данных из БД: $($Error[0])"
        break
    }

# Пишем данные из масива в Graylog
    try{
        ForEach ($item in $array){
            $message = $item.Split("|")
            $logDate = $message[2].Trim() -replace '.{7}$'
            $logDateTime = [DateTime]::ParseExact($logDate, 'dd.MM.yyyy HH:mm:ss.ff', [System.Globalization.CultureInfo]::InvariantCulture) 
            Send-PSGelfTCP -GelfServer "$($graylogServer)" -Port "$($graylogServerPort)" -ShortMessage "$($message[3])" -FullMessage "$($message[0]) $($message[2]) $($message[3])" -HostName $($hostName) -DateTime $logDateTime -AdditionalField @{OCRType = "$($message[0])"}   
        }
        return $array.Count
    }
    catch{
        Add-Content -Path $scriptLog -Value "$(CurrantDate) | Error | Ошибка загрузки на лог сервер: $($Error[0])"
        break
    }
}

# Проверки наличия файлов и папок необходимых для работы скрипта 
if (-not (Test-Path -Path "$($workFolder)\OCRScripData" -PathType Container)) {
    New-Item -ItemType Directory -Path "$($workFolder)\OCRScripData" -Force
}

if (!(Test-Path $scriptLog))
{
   New-Item -path $scriptLog -type "file"
   Add-Content -Path $scriptLog -Value "$(CurrantDate) | Info | Создание нового лога" 
}

if (!(Test-Path $tableLastRow))
{
   "0" | Set-Content -Path $tableLastRow
   Add-Content -Path $scriptLog -Value "$(CurrantDate) | Info | Создание файла для записи последней строки БД" 
}

if (!(Resolve-Path -Path $pathToDb)){
    Add-Content -Path $scriptLog -Value "$(CurrantDate) | Error |$($Error[0])"
    break
}

if (!(Test-Path $scriptLog))
{
   New-Item -path $scriptLog -type "file"
   Add-Content -Path $scriptLog -Value "$(CurrantDate) | Info | Создание нового лога" 
}

# Очистка лога больше 10мб 
$logFileSize = (Get-Item $scriptLog).length / 1MB
if ($logFileSize -gt 10) {
    Add-Content -Path $scriptLog -Value "$(CurrantDate) | Info | Замена лог-файла"
    Remove-Item $retentionLog -Force
    Move-Item -Path $scriptLog -Destination $retentionLog -Force
    Add-Content -Path $scriptLog -Value "$(CurrantDate) | Info | Начало новго лог-файла"
}

# Проверяем последнее значение в базе
[int]$dbLastRow = GetLastRow

# Проверяем последнее значние в файле
[int]$lastCheckedRow = LastCheckedRow

# Читаем\пишем логи
try{
    if ([int]$lastCheckedRow -gt "$([int]$dbLastRow)") {
        Add-Content -Path $scriptLog -Value "$(CurrantDate) | Info | Значение $($lastCheckedRow) в файле больше значения БД - ищем архив, дописываем дозабираем данные с него и обнуляем файл"
        $lastArchive = GetArchiveDbFileName
        GetDataAndWriteToGraylog -FileName "$($pathToArchiveDbFolder)/$($lastArchive)" -Row "$($lastCheckedRow)"
        "0" | Set-Content -Path $tableLastRow -NoNewline
        Add-Content -Path $scriptLog -Value "$(CurrantDate) | Info | Архив $($lastArchive) обошел - берусь за свежу новую базу $($pathToDb)"
        $newLastRow = LastCheckedRow
        GetDataAndWriteToGraylog -FileName "$($pathToDb)" -Row "$($newLastRow)"  
        "$($dbLastRow)" | Set-Content -Path "$($tableLastRow)" -NoNewline
    }
    else {
        Add-Content -Path $scriptLog -Value "$(CurrantDate) | Info | Значение $($lastCheckedRow) в файле меньше или равно значению БД - работаем с текущей БД"
        GetDataAndWriteToGraylog -FileName $pathToDb -Row "$(LastCheckedRow)"
        "$($dbLastRow)" | Set-Content -Path "$($tableLastRow)" -NoNewline
    }
}
catch {
    Add-Content -Path $scriptLog -Value "$(CurrantDate) | Error | Ошибка обработки логов: $($Error[0])"
    break
}