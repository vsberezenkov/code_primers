$hostname = ([System.Net. Dns]::GetHostName()).ToLower()
Write-Host "Start on $hostname"
$Host.UI.RawUI.WindowTitle="Проверка наличия нов айлов_$hostname"

$filesPathInform = [System.Collections.ArrayList]@()
$OnDate = Get-Date -Uformat "%d/%m/%Y"
$CurDateTime = Get-Date -UFormat "%d.%m.SY %H:%M"
$SourceDate= Get-Date (Get-Date).AddDays ($Time Shift) -Uformat "%Y%m%d%_%H%M%S"
$yesterday= (get-date).AddDays (0)
$date=get-date -Year ($yesterday. Year) -Month ($yesterday. Month) -Day ($yesterday.day) -Hour "" -Minute "" -Second ""

#!!! При эксплуатации указать целевых получателей информирования

$recipients ="defaulthuman01@gmail.com, defaulthuman02@gmail.com, defaulthuman03@gmail.com"
$recipients2="defaulthuman01@gmail.com" 

#Задаем переменные для подключения по АРІ
$Uri = 'https://sms.ru/http-api/v1/messages' 
$username = 'username'
$password = 'password'
$userpass = "$($username):$($password)" 
$userpass

#Кодируем пароль в Ваsе64, нашAPI умеет читать только его
$encodedCreds = [System.Convert]::ToBase64String ([System.Text.Encoding]::ASCII.GetBytes($userpass))
$basicAuthValue="Basic $encodedCreds"
$headers = @{Authorization = $basicAuthValue}
#Номера на которые будет проведена рассылка
$SMSnumber1 = "79181111111" #Человек01 
$SMSnumber2 = "79181111112" #Человек02 
$SMSnumber3 = "79181111113" #Человек03 
$NumberLib = $SMSnumber1, $SMSnumber2, $SMSnumber3
[string[]]$To = $recipients.Split(',') 
[string[]]$To2 = $recipients2.Split(',')

$FullPath = "Y:\trololo"
$pathPattern = "CNT|MSK|FEA|NWE|SIB|STH|URL|VLG" #!!! Первый уровень просмотра каталогов 
$nextPathPattern = "CH01|CH02|CH04|CH06|CH08|CH10" #!!! Второй уровень просмотра каталогов 
$comparePattern = $nextPathPattern -split "\"
$compareNothing =[System.Collections.ArrayList]@()
$targetFiles = "*.csv"
$mailSubj = "Контроль выгрузки чего-то там от $CurDateTime"
$mailBody = "Внимание! `n отсутствуют актуальные данные чего-то там ($hostname) по пути: `n"

Get-ChildItem $FullPath -Recurse -Directory -Depth 01 Where-Object {($_.FullName -match $pathPattern)} foreach-object { 
    $CurDir $_.FullName
    $prop1 @{Expression='Name'; Ascending=$true}
    $prop2 @{Expression='LastWriteTime'; Descending=$true}
    $compareMask = [System.Collections.ArrayList]@()

    $rndColl = Get-Random -InputObject "Work it harder", "Make it better", "Do it faster", "Make us stronger", "More then ever", "Hour after hour", "Work is never over"
    Write-Host $rndColl

    Get-ChildItem $CurDir -Directory -Recurse | Where-Object {($_.FullName -match $nextPathPattern)} | Sort-Object $prop1, $prop2| foreach-object { 
        $FilerDir = $_.FullName
            
            Get-ChildItem $FilerDir -File -Include $targetFiles -Recurse | Where-Object {($_.FullName -match $filename Pattern) and ($_. LastWriteTime -gt $date) } select first 1 | foreach-object {
            $found = $_ | select name, fullname, @{n='Region' ;e={$_.FullName.substring (24,3)} }, @{n='CH';e={$_.FullName.substring (28,4)} }
                if ($found.CH -notin $compareMask) {
                    #$found
                    $compareMask+=$found
                    Write-Host "<<<"
                    $compareMask
                    Write-Host ">>>"
                }
            }
    $compareNothing =$comparePattern | Where-Object { $_ -notin $compareMask.CH} | ForEach-Object {"$($FilerDir.substring(0, 12))"+"\"+"$_"}
    }
    ForEach-Object {if ($compareNothing -ne "") { $filesPathInform+=$compareNothing}}
}

$filesPathInform
Write-Host "..."

$PSDefaultParameterValues['Out-File:Encoding'] = '1251'

if ($filesPathInform.Count -gt 0) {
    $mailBodyInfo = $filesPathInform -join [Environment]:: NewLine
    $mailBody = ($mailBody + ($filesPathInform -join [Environment]:: NewLine))
    $mailBodySMS = "Статус от $CurDateTime. Отсутствуют актуальные данные чего-то там ($hostname) по пути: $filesPathInform" -replace '\\', ''
    $mailBodySMS

    Send-MailMessage -To $To -From "smtp@mail.ru" -subject $mailSubj -Body $mailBody -Encoding UTF8 -smtpServer mail.inside.nothing.ru -Port 25 
    
    #Тело сообщения отправляемого через API циклом по библиотеке номеров $NumberLib
    ForEach ($Number in $NumberLib)
    {
        $body = '{"messages": [{"content":{"short_text": "'+$mailBodySMS+'"},
                                "to":[{"msisdn":"'+$Number+'"}
                                1,
                                "from":{"sms_address":"MTC"}
                                }
                                else {
                                }
                }'
    Write-Host "Отправка SMS на +$Number"
    $Result = Invoke-RestMethod -Uri $Uri -Method Post -Headers $headers -ContentType "application/json" -Body $body 
    Start-Sleep -Seconds 1
    }
    Write-Host "Done NOK"
}
else {
        $mailBody = "Вce OK"
        Send-MailMessage -To $To2 -From "smtp@mail.ru" -subject $mailSubj -Body $mailBody -Encoding UTF8 -smtpServer mail.inside.nothing.ru -Port 25
        Write-Host "Done Ok"
}