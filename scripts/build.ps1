# Windows PowerShell equivalent of build.sh.  Run from the repo root:  .\scripts\build.ps1
$ErrorActionPreference = "Stop"
Remove-Item -Recurse -Force dist, build -ErrorAction SilentlyContinue
New-Item -ItemType Directory -Force dist, build\layer\python | Out-Null
pip install --quiet "pg8000==1.31.*" -t build\layer\python
Compress-Archive -Path build\layer\python -DestinationPath dist\pg8000-layer.zip
foreach ($fn in "plan_dates","ingest_bronze","load_gold","api") {
  New-Item -ItemType Directory -Force "build\$fn" | Out-Null
  Copy-Item "lambdas\$fn\handler.py" "build\$fn\"
  if ($fn -eq "load_gold") { Copy-Item sql\postgres_schema.sql build\load_gold\ }
  Compress-Archive -Path "build\$fn\*" -DestinationPath "dist\$fn.zip"
}
Get-ChildItem dist
