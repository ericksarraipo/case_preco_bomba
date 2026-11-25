$ErrorActionPreference = "Stop"

$ProjectRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $ProjectRoot

$VenvPython = Join-Path $ProjectRoot ".venv\Scripts\python.exe"
if (Test-Path $VenvPython) {
    $PythonExe = $VenvPython
} else {
    $PythonExe = "python"
}

function Invoke-JobStep {
    param(
        [string]$StepName,
        [string]$ModuleName,
        [string]$ExpectedFile
    )
    $ScriptPath = Join-Path $ProjectRoot $ExpectedFile
    if (-not (Test-Path $ScriptPath)) {
        throw "Arquivo esperado nao encontrado para '$StepName': $ExpectedFile"
    }

    Write-Host ""
    Write-Host "=== [$StepName] Executando $ModuleName ===" -ForegroundColor Cyan
    & $PythonExe -m $ModuleName
    if ($LASTEXITCODE -ne 0) {
        throw "Falha na etapa '$StepName' ($ModuleName) com exit code $LASTEXITCODE."
    }
}

Write-Host "Iniciando pipeline Distribuidora Veredas em: $ProjectRoot" -ForegroundColor Green

# 1. Bronze (ingestao)
if (Test-Path (Join-Path $ProjectRoot "src\jobs\ingest_bronze.py")) {
    Invoke-JobStep -StepName "BRONZE" -ModuleName "src.jobs.ingest_bronze" -ExpectedFile "src\jobs\ingest_bronze.py"
} elseif (Test-Path (Join-Path $ProjectRoot "src\jobs\run_bronze.py")) {
    Invoke-JobStep -StepName "BRONZE" -ModuleName "src.jobs.run_bronze" -ExpectedFile "src\jobs\run_bronze.py"
} else {
    Write-Host "[AVISO] Script de ingestao Bronze nao encontrado em src\jobs\; assumindo dados Bronze/Prata ja presentes." -ForegroundColor Yellow
}

# 2. Prata (transformacoes)
$SilverJobs = @(
    @{ Name = "SILVER IPCA";      Module = "src.jobs.transform_silver_ipca_mensal";         File = "src\jobs\transform_silver_ipca_mensal.py" },
    @{ Name = "SILVER IBGE";      Module = "src.jobs.transform_silver_municipios_ibge";     File = "src\jobs\transform_silver_municipios_ibge.py" },
    @{ Name = "SILVER REEMBOLSO"; Module = "src.jobs.transform_silver_reembolso_uf_vigencia"; File = "src\jobs\transform_silver_reembolso_uf_vigencia.py" },
    @{ Name = "SILVER PRECOS";    Module = "src.jobs.transform_silver_precos_combustiveis"; File = "src\jobs\transform_silver_precos_combustiveis.py" }
)

foreach ($job in $SilverJobs) {
    Invoke-JobStep -StepName $job.Name -ModuleName $job.Module -ExpectedFile $job.File
}

# 3. Ouro + Qualidade + Relatorio HTML
Invoke-JobStep -StepName "GOLD"     -ModuleName "src.jobs.transform_gold_precos_reembolso" -ExpectedFile "src\jobs\transform_gold_precos_reembolso.py"
Invoke-JobStep -StepName "VALIDATE" -ModuleName "src.jobs.validate_gold"                 -ExpectedFile "src\jobs\validate_gold.py"

$ReportScript = Join-Path $ProjectRoot "src\jobs\generate_report_html.py"
$ReportHtml   = Join-Path $ProjectRoot "reports\relatorio_distribuidora_veredas.html"
if (Test-Path $ReportScript) {
    Invoke-JobStep -StepName "REPORT HTML" -ModuleName "src.jobs.generate_report_html" -ExpectedFile "src\jobs\generate_report_html.py"
} elseif (Test-Path $ReportHtml) {
    Write-Host ""
    Write-Host "[AVISO] src\jobs\generate_report_html.py nao encontrado; mantendo relatorio existente em: $ReportHtml" -ForegroundColor Yellow
} else {
    throw "Relatorio HTML nao encontrado em $ReportHtml e script ausente em $ReportScript"
}

Write-Host ""
Write-Host "[SUCESSO] Pipeline concluido! Relatorio em: reports\relatorio_distribuidora_veredas.html" -ForegroundColor Green

if (-not $NoBrowser -and (Test-Path $ReportHtml)) {
    Start-Process $ReportHtml
}