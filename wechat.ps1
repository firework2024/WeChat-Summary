param(
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]] $ArgsList
)

$exe = Join-Path $PSScriptRoot ".venv\Scripts\wechat-cli.exe"
if (-not (Test-Path $exe)) {
    Write-Error "wechat-cli virtual environment was not found. Expected: $exe"
    exit 1
}

& $exe @ArgsList
exit $LASTEXITCODE
