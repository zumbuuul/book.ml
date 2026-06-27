dotnet user-secrets init
dotnet user-secrets set "API_KEY" "kljuc"

testing linux:
seq 1 10 | xargs -I {} -P 10 curl -s -o /dev/null -w "Request {}: %{http_code}\n" "http://localhost:3000/?q=crime%20and%20punishment,waiting%20for%20godot"

testing windows:
$url = "http://localhost:3000/?q=crime%20and%20punishment,waiting%20for%20godot"

1..10 | ForEach-Object {
Start-Job -ScriptBlock {
param($i, $url)
        curl.exe -s -o NUL -w "Request $($i): %{http*code}`n" $url
} -ArgumentList $*, $url
} | Receive-Job -Wait -AutoRemoveJob
