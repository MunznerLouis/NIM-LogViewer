[CmdletBinding()]
param(
    [string]$BasePath = "C:\Usercube\Work\ProvisioningOrders",
    [DateTime]$StartDate,
    [DateTime]$EndDate,
    [switch]$Serve,
    [int]$Port = 5000,
    [Alias('h')][switch]$Help
)

# ═══════════════════════════════════════════════════════════════════════════════
# SERVE MODE — helper functions (only used when -Serve is passed)
# ═══════════════════════════════════════════════════════════════════════════════

function Send-Response {
    param($Response, [byte[]]$Bytes, [string]$ContentType)
    $Response.ContentType     = $ContentType
    $Response.ContentLength64 = $Bytes.Length
    $Response.OutputStream.Write($Bytes, 0, $Bytes.Length)
    $Response.Close()
}

function ConvertFrom-QueryString {
    param([string]$Query)
    $result = @{}
    $q = $Query.TrimStart('?')
    if (-not $q) { return $result }
    foreach ($pair in ($q -split '&')) {
        $parts = $pair -split '=', 2
        $key   = [System.Uri]::UnescapeDataString($parts[0].Replace('+', ' '))
        $val   = if ($parts.Count -gt 1) { [System.Uri]::UnescapeDataString($parts[1].Replace('+', ' ')) } else { '' }
        $result[$key] = $val
    }
    return $result
}

function Send-JsonResponse {
    param($Response, [string]$Json)
    $bytes = [System.Text.Encoding]::UTF8.GetBytes($Json)
    Send-Response -Response $Response -Bytes $bytes -ContentType 'application/json; charset=utf-8'
}

function Get-OrderMeta {
    param([System.Collections.ArrayList]$Orders)
    $se   = @{}; $te = @{}
    $s2t  = @{}; $t2s = @{}
    foreach ($d in $Orders) {
        $s = if ($d.sourceEntity) { $d.sourceEntity } else { 'Unknown' }
        $t = if ($d.targetEntity) { $d.targetEntity } else { 'Unknown' }
        $se[$s] = 1; $te[$t] = 1
        if (-not $s2t.ContainsKey($s)) { $s2t[$s] = @{} }
        if (-not $t2s.ContainsKey($t)) { $t2s[$t] = @{} }
        $s2t[$s][$t] = $true
        $t2s[$t][$s] = $true
    }
    # Build nested link maps
    $s2tOut = [ordered]@{}
    foreach ($sk in ($s2t.Keys | Sort-Object)) {
        $s2tOut[$sk] = [ordered]@{}
        foreach ($tk in $s2t[$sk].Keys) { $s2tOut[$sk][$tk] = $true }
    }
    $t2sOut = [ordered]@{}
    foreach ($tk in ($t2s.Keys | Sort-Object)) {
        $t2sOut[$tk] = [ordered]@{}
        foreach ($sk in $t2s[$tk].Keys) { $t2sOut[$tk][$sk] = $true }
    }
    return [ordered]@{
        sourceEntities = @($se.Keys | Sort-Object)
        targetEntities = @($te.Keys | Sort-Object)
        entityLinks    = [ordered]@{ s2t = $s2tOut; t2s = $t2sOut }
        totalOrders    = $Orders.Count
    }
}

function Get-OrderStats {
    param($Orders)
    $s = [ordered]@{ total = $Orders.Count; added = 0; modified = 0; deleted = 0; rtCount = 0 }
    $rt = @{}
    foreach ($d in $Orders) {
        $ct = if ($d.changeType) { $d.changeType.ToLower() } else { '' }
        if      ($ct -eq 'added')    { $s.added++ }
        elseif  ($ct -eq 'modified') { $s.modified++ }
        elseif  ($ct -eq 'deleted')  { $s.deleted++ }
        $rt[$(if ($d.resourceType) { $d.resourceType } elseif ($d.resourceTypeDir) { $d.resourceTypeDir } else { '?' })] = 1
    }
    $s.rtCount = $rt.Count
    return $s
}

function Get-OrderTimeline {
    param($Orders)
    $map = [ordered]@{}
    foreach ($d in $Orders) {
        $fd = if ($d.fileDate -is [datetime]) { $d.fileDate.ToString('yyyy-MM-dd') } elseif ($d.fileDate) { "$($d.fileDate)" } else { '' }
        $dk = if ($fd.Length -ge 10) { $fd.Substring(0, 10) } else { '?' }
        if (-not $map.Contains($dk)) { $map[$dk] = [ordered]@{ added = 0; modified = 0; deleted = 0 } }
        $ct = if ($d.changeType) { $d.changeType.ToLower() } else { '' }
        if      ($ct -eq 'added')    { $map[$dk].added++ }
        elseif  ($ct -eq 'modified') { $map[$dk].modified++ }
        elseif  ($ct -eq 'deleted')  { $map[$dk].deleted++ }
    }
    return @($map.Keys | Sort-Object | ForEach-Object {
        $v = $map[$_]
        [ordered]@{ date = $_; added = $v.added; modified = $v.modified; deleted = $v.deleted; total = $v.added + $v.modified + $v.deleted }
    })
}

function Invoke-OrderQuery {
    param([System.Collections.ArrayList]$Orders, $QueryString)

    # ── Parse query params ──────────────────────────────────────────────────
    $rawPage  = $QueryString['page'];     $pageNum  = if ($rawPage  -match '^\d+$') { [int]$rawPage  } else { 0 }
    $rawSize  = $QueryString['pageSize']; $pageSize = if ($rawSize  -match '^\d+$') { [int]$rawSize  } else { 50 }
    if ($pageSize -lt 1 -or $pageSize -gt 500) { $pageSize = 50 }

    $sortCol  = $QueryString['sortCol'];  if (-not $sortCol)  { $sortCol  = 'fileDate' }
    $sortDir  = $QueryString['sortDir'];  if ($sortDir -ne 'asc') { $sortDir = 'desc' }

    $seFilter = $QueryString['sourceEntity']
    $teFilter = $QueryString['targetEntity']
    $search   = $QueryString['search']
    $dateFrom = $QueryString['dateFrom']
    $dateTo   = $QueryString['dateTo']
    $chMin    = $QueryString['changesMin']
    $chMax    = $QueryString['changesMax']
    $chAttr   = $QueryString['changesAttr']
    $chVal    = $QueryString['changesValue']
    $rlMin    = $QueryString['rolesMin']
    $rlMax    = $QueryString['rolesMax']
    $rlDir    = $QueryString['rolesDir']

    $ctRaw    = $QueryString['changeTypes']
    $ctOn     = @{ Added = $true; Modified = $true; Deleted = $true }
    if ($ctRaw) {
        $ctOn = @{ Added = $false; Modified = $false; Deleted = $false }
        $ctRaw -split ',' | ForEach-Object { $ctOn[$_.Trim()] = $true }
    }

    # ── Filter ──────────────────────────────────────────────────────────────
    $filtered = @($Orders | Where-Object {
        $d = $_

        if ($seFilter -and $seFilter -ne '__all__') {
            $se = if ($d.sourceEntity) { $d.sourceEntity } else { 'Unknown' }
            if ($se -ne $seFilter) { return $false }
        }
        if ($teFilter -and $teFilter -ne '__all__') {
            $te = if ($d.targetEntity) { $d.targetEntity } else { 'Unknown' }
            if ($te -ne $teFilter) { return $false }
        }

        $ct   = if ($d.changeType) { $d.changeType } else { '' }
        $norm = if ($ct.Length -gt 0) { $ct.Substring(0,1).ToUpper() + $ct.Substring(1).ToLower() } else { '' }
        if (-not $ctOn[$norm]) { return $false }

        if ($search) {
            $q   = $search.ToLower()
            $hay = @($d.ownerName, $d.ownerIdentifier, $d.fileName, $d.sourceEntity,
                     $d.targetEntity, $d.resourceType, $d.resourceTypeDir) | Where-Object { $_ -ne $null }
            if (($hay -join ' ').ToLower().IndexOf($q) -lt 0) { return $false }
        }

        $fd = if ($d.fileDate -is [datetime]) { $d.fileDate.ToString('yyyy-MM-ddTHH:mm:ss') } elseif ($d.fileDate) { "$($d.fileDate)" } else { '' }
        if ($dateFrom -and $fd -and $fd -lt $dateFrom) { return $false }
        if ($dateTo   -and $fd -and $fd -gt ($dateTo + 'T23:59:59')) { return $false }

        $nch = if ($d.changes) { $d.changes.Count } else { 0 }
        if ($chMin -and $chMin -ne '' -and $nch -lt [int]$chMin) { return $false }
        if ($chMax -and $chMax -ne '' -and $nch -gt [int]$chMax) { return $false }
        if ($chAttr -and $chAttr -ne '__all__') {
            if (-not ($d.changes -and $d.changes.Contains($chAttr))) { return $false }
        }
        if ($chVal -and $chVal -ne '') {
            $q = $chVal.ToLower(); $found = $false
            if ($d.changes) {
                foreach ($k in $d.changes.Keys) {
                    $v = $d.changes[$k]
                    if ($null -ne $v -and $v.ToString().ToLower().IndexOf($q) -ge 0) { $found = $true; break }
                }
            }
            if (-not $found) { return $false }
        }

        $roles = if ($d.roles) { @($d.roles) } else { @() }
        $nrl   = if ($rlDir -and $rlDir -ne '__all__') { ($roles | Where-Object { $_.direction -eq $rlDir }).Count } else { $roles.Count }
        if ($rlDir -and $rlDir -ne '__all__' -and $nrl -eq 0) { return $false }
        if ($rlMin -and $rlMin -ne '' -and $nrl -lt [int]$rlMin) { return $false }
        if ($rlMax -and $rlMax -ne '' -and $nrl -gt [int]$rlMax) { return $false }

        return $true
    })

    # ── Stats & timeline from full filtered set ─────────────────────────────
    $stats    = Get-OrderStats    -Orders $filtered
    $timeline = Get-OrderTimeline -Orders $filtered

    # ── Available attrs for the current target entity filter ────────────────
    $availableAttrs = @()
    if ($teFilter -and $teFilter -ne '__all__') {
        $attrSeen = @{}
        foreach ($d in $filtered) {
            if ($d.changes) { foreach ($k in $d.changes.Keys) { $attrSeen[$k] = 1 } }
        }
        $availableAttrs = @($attrSeen.Keys | Sort-Object)
    }

    # ── Sort ────────────────────────────────────────────────────────────────
    $desc   = ($sortDir -eq 'desc')
    $sorted = switch ($sortCol) {
        'changesCount' { $filtered | Sort-Object { if ($_.changes) { $_.changes.Count } else { 0 } } -Descending:$desc }
        'rolesCount'   {
            $filtered | Sort-Object {
                $roles = if ($_.roles) { @($_.roles) } else { @() }
                if ($rlDir -and $rlDir -ne '__all__') { ($roles | Where-Object { $_.direction -eq $rlDir }).Count }
                else { $roles.Count }
            } -Descending:$desc
        }
        default        {
            $filtered | Sort-Object {
                $v = $_.$sortCol
                if ($v -is [datetime]) { $v.ToString('yyyy-MM-ddTHH:mm:ss') } elseif ($v) { "$v" } else { '' }
            } -Descending:$desc
        }
    }

    # ── Paginate ────────────────────────────────────────────────────────────
    $records = @($sorted) | Select-Object -Skip ($pageNum * $pageSize) -First $pageSize

    return [ordered]@{
        total          = $filtered.Count
        page           = $pageNum
        pageSize       = $pageSize
        records        = @($records)
        stats          = $stats
        timeline       = $timeline
        availableAttrs = $availableAttrs
    }
}

function Get-LifecycleData {
    param([System.Collections.ArrayList]$Orders, [string]$Search)
    if (-not $Search -or $Search.Length -lt 2) {
        return [ordered]@{ owners = @(); total = 0; truncated = $false }
    }
    $q      = $Search.ToLower()
    $owners = [ordered]@{}
    foreach ($d in $Orders) {
        $name = if ($d.ownerName)       { $d.ownerName }       else { 'Unknown' }
        $id   = if ($d.ownerIdentifier) { $d.ownerIdentifier } else { '' }
        if ($name.ToLower().IndexOf($q) -lt 0 -and $id.ToLower().IndexOf($q) -lt 0) { continue }
        $key  = if ($d.ownerIdentifier) { $d.ownerIdentifier } else { $name }
        if (-not $owners.Contains($key)) {
            $owners[$key] = [ordered]@{ name = $name; id = $id; orders = [System.Collections.ArrayList]::new() }
        }
        [void]$owners[$key].orders.Add($d)
    }
    $list = @($owners.Values | Sort-Object { $_.name })
    foreach ($ow in $list) { $ow.orders = @($ow.orders | Sort-Object { $_.fileDate }) }
    return [ordered]@{
        owners    = @($list | Select-Object -First 50)
        total     = $list.Count
        truncated = ($list.Count -gt 50)
    }
}

function Start-LogViewerServer {
    param([System.Collections.ArrayList]$AllOrders, [string]$HtmlTemplate, [int]$Port)

    # Prevent Stop-mode errors from crashing request handling
    $ErrorActionPreference = 'Continue'

    # Prepare serve-mode HTML — replace data placeholder with null + API base
    $serveHtml   = $HtmlTemplate.Replace(
        'window.__RAW_DATA__ = __DATA_PLACEHOLDER__;',
        "window.__RAW_DATA__ = null; window.__API_BASE__ = 'http://localhost:$Port';"
    )
    $serveHtmlBytes = [System.Text.Encoding]::UTF8.GetBytes($serveHtml)

    # Pre-compute metadata (static — doesn't change between requests)
    Write-Host "Pre-computing metadata..." -ForegroundColor Gray
    $meta      = Get-OrderMeta -Orders $AllOrders
    $metaJson  = $meta | ConvertTo-Json -Depth 10 -Compress
    $metaBytes = [System.Text.Encoding]::UTF8.GetBytes($metaJson)

    $listener = New-Object System.Net.HttpListener
    $listener.Prefixes.Add("http://localhost:$Port/")

    try {
        $listener.Start()
    } catch {
        Write-Error "Could not start HTTP server on port $Port. The port may already be in use."
        return
    }

    $url = "http://localhost:$Port"
    Write-Host ""
    Write-Host "  LogViewer running at: $url" -ForegroundColor Green
    Write-Host "  Orders loaded: $($AllOrders.Count)" -ForegroundColor Cyan
    Write-Host "  Press Ctrl+C to stop." -ForegroundColor Gray
    Write-Host ""

    Start-Process $url

    try {
        while ($listener.IsListening) {
            # Non-blocking wait: poll every 300ms so Ctrl+C is processed between checks
            $async = $listener.BeginGetContext($null, $null)
            while (-not $async.AsyncWaitHandle.WaitOne(300)) {
                if (-not $listener.IsListening) { break }
            }
            if (-not $listener.IsListening) { break }

            $context = $null
            try {
                $context = $listener.EndGetContext($async)
            } catch [System.Net.HttpListenerException] {
                break
            }
            if (-not $context) { continue }

            $req  = $context.Request
            $resp = $context.Response
            $path = $req.Url.LocalPath

            Write-Verbose "  --> $($req.HttpMethod) $path$($req.Url.Query)"

            try {
                switch -Regex ($path) {
                    '^/$|^/index\.html$' {
                        Send-Response -Response $resp -Bytes $serveHtmlBytes -ContentType 'text/html; charset=utf-8'
                        Write-Verbose "  <-- 200 HTML ($($serveHtmlBytes.Length) bytes)"
                    }
                    '^/api/meta$' {
                        Send-Response -Response $resp -Bytes $metaBytes -ContentType 'application/json; charset=utf-8'
                        Write-Verbose "  <-- 200 meta ($($metaBytes.Length) bytes)"
                    }
                    '^/api/query$' {
                        $qs     = ConvertFrom-QueryString $req.Url.Query
                        $result = Invoke-OrderQuery -Orders $AllOrders -QueryString $qs
                        $json   = $result | ConvertTo-Json -Depth 10 -Compress
                        Send-JsonResponse -Response $resp -Json $json
                        Write-Verbose "  <-- 200 query ($($result.total) total, page $($result.page))"
                    }
                    '^/api/lifecycle$' {
                        $qs     = ConvertFrom-QueryString $req.Url.Query
                        $result = Get-LifecycleData -Orders $AllOrders -Search $qs['search']
                        $json   = $result | ConvertTo-Json -Depth 10 -Compress
                        Send-JsonResponse -Response $resp -Json $json
                        Write-Verbose "  <-- 200 lifecycle"
                    }
                    default {
                        $resp.StatusCode = 404
                        Send-JsonResponse -Response $resp -Json '{"error":"not found"}'
                        Write-Verbose "  <-- 404 $path"
                    }
                }
            } catch {
                Write-Warning "Request failed - $($req.HttpMethod) $path : $_"
                try {
                    $resp.StatusCode = 500
                    $errBytes = [System.Text.Encoding]::UTF8.GetBytes('{"error":"internal server error"}')
                    $resp.ContentType = 'application/json; charset=utf-8'
                    $resp.ContentLength64 = $errBytes.Length
                    $resp.OutputStream.Write($errBytes, 0, $errBytes.Length)
                } catch {
                    Write-Warning "Could not send error response: $_"
                } finally {
                    try { $resp.Close() } catch {}
                }
            }
        }
    } finally {
        if ($listener.IsListening) { $listener.Stop() }
        $listener.Close()
        Write-Host "Server stopped." -ForegroundColor Gray
    }
}

# ═══════════════════════════════════════════════════════════════════════════════
# HELP
# ═══════════════════════════════════════════════════════════════════════════════

if ($Help) {
    Write-Host ""
    Write-Host "  Usercube Provisioning LogViewer" -ForegroundColor Cyan
    Write-Host "  ===============================" -ForegroundColor Cyan
    Write-Host ""
    Write-Host "  Scans provisioning order JSON files and generates an interactive HTML viewer." -ForegroundColor Gray
    Write-Host ""
    Write-Host "  USAGE:" -ForegroundColor Yellow
    Write-Host "    .\LogViewer.ps1 [options]"
    Write-Host ""
    Write-Host "  OPTIONS:" -ForegroundColor Yellow
    Write-Host "    -BasePath <path>      Root folder containing provisioning orders."
    Write-Host "                          Default: C:\Usercube\Work\ProvisioningOrders"
    Write-Host ""
    Write-Host "    -StartDate <date>     Only include orders from this date onward."
    Write-Host "                          Format: yyyy-MM-dd (e.g. 2026-03-01)"
    Write-Host ""
    Write-Host "    -EndDate <date>       Only include orders up to this date."
    Write-Host "                          Format: yyyy-MM-dd (e.g. 2026-03-31)"
    Write-Host ""
    Write-Host "    -Serve                Start a local HTTP server instead of generating a file."
    Write-Host "                          Best for large datasets. Opens browser automatically."
    Write-Host "                          Press Ctrl+C to stop the server when done."
    Write-Host ""
    Write-Host "    -Port <number>        Port for the HTTP server (default: 5000)."
    Write-Host "                          Only used with -Serve."
    Write-Host ""
    Write-Host "    -Help, -h             Show this help message."
    Write-Host ""
    Write-Host "  EXAMPLES:" -ForegroundColor Yellow
    Write-Host "    .\LogViewer.ps1                                              # All orders, generate HTML"
    Write-Host "    .\LogViewer.ps1 -StartDate 2026-03-01 -EndDate 2026-03-31    # March only, generate HTML"
    Write-Host "    .\LogViewer.ps1 -Serve                                       # All orders, local server"
    Write-Host "    .\LogViewer.ps1 -Serve -StartDate 2026-01-01 -Port 8080      # Server on custom port"
    Write-Host ""
    Write-Host "  WHEN TO USE -Serve:" -ForegroundColor Yellow
    Write-Host "    Use -Serve when the dataset is too large for a self-contained HTML file"
    Write-Host "    (rough limit: ~100k orders). The server holds data in memory and handles"
    Write-Host "    all filtering server-side, so the browser only receives the current page."
    Write-Host ""
    Write-Host "  OUTPUT:" -ForegroundColor Yellow
    Write-Host "    Default: generates LogViewer.html next to this script."
    Write-Host "    -Serve:  starts http://localhost:5000, opens browser, stays alive until Ctrl+C."
    Write-Host ""
    return
}

# ═══════════════════════════════════════════════════════════════════════════════
# FILE SCANNING & PROCESSING
# ═══════════════════════════════════════════════════════════════════════════════

$OutputPath = Join-Path $PSScriptRoot "LogViewer.html"

$ErrorActionPreference = 'Stop'

$dateInfo = ""
if ($StartDate) { $dateInfo += "from $($StartDate.ToString('yyyy-MM-dd')) " }
if ($EndDate)   { $dateInfo += "to $($EndDate.ToString('yyyy-MM-dd'))" }
if (-not $dateInfo) { $dateInfo = "all dates" }

Write-Host "Scanning $BasePath ($dateInfo) ..." -ForegroundColor Cyan

if (-not (Test-Path $BasePath)) {
    Write-Error "Base path not found: $BasePath"
    return
}

$allJsonFiles = Get-ChildItem -Path $BasePath -Filter "*.json" -File -Recurse |
    Where-Object {
        if ($_.BaseName -match '^(\d{14})') {
            try {
                $fileDate = [datetime]::ParseExact($Matches[1], 'yyyyMMddHHmmss', $null)
                if ($StartDate -and $fileDate -lt $StartDate)               { return $false }
                if ($EndDate   -and $fileDate -gt $EndDate.Date.AddDays(1)) { return $false }
                return $true
            } catch { return $false }
        }
        return $false
    } |
    Sort-Object FullName

if ($allJsonFiles.Count -eq 0) {
    Write-Warning "No provisioning order JSON files found ($dateInfo) in $BasePath"
    return
}

Write-Host "Found $($allJsonFiles.Count) JSON file(s) to process." -ForegroundColor Cyan

$allOrders  = [System.Collections.ArrayList]::new()
$totalFiles = $allJsonFiles.Count
$processed  = 0
$lastPct    = -1

foreach ($jsonFile in $allJsonFiles) {
    $processed++
    $pct = [math]::Floor(($processed / $totalFiles) * 100)
    if ($pct -ne $lastPct -and ($pct % 5 -eq 0 -or $processed -eq $totalFiles)) {
        Write-Host "  [$pct%] $processed / $totalFiles files" -ForegroundColor Gray
        $lastPct = $pct
    }

    $isoDate = $null
    if ($jsonFile.BaseName -match '^(\d{14})') {
        try {
            $parsed  = [datetime]::ParseExact($Matches[1], 'yyyyMMddHHmmss', $null)
            $isoDate = $parsed.ToString('yyyy-MM-ddTHH:mm:ss')
        } catch { continue }
    } else { continue }

    $resourceTypeDir = $jsonFile.Directory.Name

    try {
        $content = Get-Content -Path $jsonFile.FullName -Raw -Encoding UTF8
        $json    = $content | ConvertFrom-Json
    } catch { continue }

    $orders = @()
    if ($json.ProvisioningOrdersList) { $orders = @($json.ProvisioningOrdersList) }
    else                              { $orders = @($json) }

    foreach ($order in $orders) {
        $record = [ordered]@{
            fileName        = $jsonFile.Name
            fileDate        = $isoDate
            resourceTypeDir = $resourceTypeDir
            changeType      = $order.ChangeType
            resourceType    = $null
            sourceEntity    = $null
            targetEntity    = $null
            ownerName       = $null
            ownerIdentifier = $null
            changes         = @{}
            roles           = @()
            owner           = @{}
        }

        if ($order.ResourceType) {
            $rt = $order.ResourceType
            $record.resourceType = if ($rt.Identifier) { $rt.Identifier } else { $rt.Id }
            if ($rt.SourceEntityType) {
                $record.sourceEntity = if ($rt.SourceEntityType.Identifier) { $rt.SourceEntityType.Identifier } else { $rt.SourceEntityType.Id }
            }
            if ($rt.TargetEntityType) {
                $record.targetEntity = if ($rt.TargetEntityType.Identifier) { $rt.TargetEntityType.Identifier } else { $rt.TargetEntityType.Id }
            }
        }

        if ($order.Owner) {
            $o = $order.Owner
            $record.ownerName = if ($o.InternalDisplayName) { $o.InternalDisplayName }
                                elseif ($o.DisplayName)     { $o.DisplayName }
                                elseif ($o.LastFirstName)   { $o.LastFirstName }
                                else                        { $o.Id }
            $record.ownerIdentifier = $o.Identifier
            $ownerHash = [ordered]@{}
            $o.PSObject.Properties | ForEach-Object { $ownerHash[$_.Name] = $_.Value }
            $record.owner = $ownerHash
        }

        if ($order.Changes) {
            $changesHash = [ordered]@{}
            $rolesArr    = [System.Collections.ArrayList]::new()
            $order.Changes.PSObject.Properties | ForEach-Object {
                $propName = $_.Name
                $propVal  = $_.Value
                $isArray  = $propVal -is [System.Collections.IEnumerable] -and -not ($propVal -is [string])
                if ($isArray) {
                    $direction = 'unknown'
                    if ($propName -match '_add$')    { $direction = 'add'    }
                    elseif ($propName -match '_remove$') { $direction = 'remove' }
                    foreach ($item in $propVal) {
                        $roleName = if     ($item -is [string]) { $item }
                                    elseif ($item.DisplayName)  { $item.DisplayName }
                                    elseif ($item.Identifier)   { $item.Identifier }
                                    elseif ($item.Id)           { $item.Id }
                                    else                        { ($item | ConvertTo-Json -Compress) }
                        [void]$rolesArr.Add([ordered]@{ name = $roleName; key = $propName; direction = $direction })
                    }
                } else {
                    $changesHash[$propName] = $propVal
                }
            }
            $record.changes = $changesHash
            $record.roles   = @($rolesArr)
        }

        [void]$allOrders.Add($record)
    }
}

if ($allOrders.Count -eq 0) {
    Write-Warning "No provisioning orders found. No output generated."
    return
}

Write-Host "Loaded $($allOrders.Count) provisioning orders." -ForegroundColor Green

# ═══════════════════════════════════════════════════════════════════════════════
# OUTPUT — serve mode or file mode
# ═══════════════════════════════════════════════════════════════════════════════

$templatePath = Join-Path $PSScriptRoot "LogViewer_base.html"
if (-not (Test-Path $templatePath)) {
    Write-Error "LogViewer_base.html not found next to the script at $PSScriptRoot"
    return
}
$htmlTemplate = Get-Content -Path $templatePath -Raw -Encoding UTF8

if ($Serve) {
    Start-LogViewerServer -AllOrders $allOrders -HtmlTemplate $htmlTemplate -Port $Port
} else {
    # Warn if dataset is large enough to stress the browser
    if ($allOrders.Count -gt 80000) {
        Write-Warning "$($allOrders.Count) orders exceeds the recommended limit (~80k) for self-contained HTML."
        Write-Warning "The file may be slow or fail to open. Consider using -Serve for this dataset size."
    }

    Write-Host "Serializing JSON..." -ForegroundColor Gray
    $jsonData = $allOrders | ConvertTo-Json -Depth 10 -Compress

    Write-Host "Writing HTML..." -ForegroundColor Gray
    $htmlFinal = $htmlTemplate.Replace('__DATA_PLACEHOLDER__', $jsonData)
    [System.IO.File]::WriteAllText($OutputPath, $htmlFinal, [System.Text.Encoding]::UTF8)

    Write-Host "`nDone: $OutputPath" -ForegroundColor Green
    Write-Host "Orders: $($allOrders.Count)" -ForegroundColor Cyan
    Start-Process $OutputPath
}
