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
# SHARED HELPERS
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

# Builds a flat record hash from a raw parsed JSON order object.
# Used by both file mode (scanning loop) and serve mode (per-query streaming).
function New-OrderRecord {
    param($Order, [string]$FileName, [string]$IsoDate, [string]$ResourceTypeDir)

    $record = [ordered]@{
        fileName            = $FileName
        fileDate            = $IsoDate
        resourceTypeDir     = $ResourceTypeDir
        changeType          = $Order.ChangeType
        resourceType        = $null
        sourceEntity        = $null
        targetEntity        = $null
        ownerName           = $null
        ownerIdentifier     = $null
        resourceDisplayName = $null
        resourceIdentifier  = $null
        changes             = @{}
        roles               = @()
        owner               = @{}
        resource            = @{}
    }

    if ($Order.ResourceType) {
        $rt = $Order.ResourceType
        $record.resourceType = if ($rt.Identifier) { $rt.Identifier } else { $rt.Id }
        if ($rt.SourceEntityType) {
            $record.sourceEntity = if ($rt.SourceEntityType.Identifier) { $rt.SourceEntityType.Identifier } else { $rt.SourceEntityType.Id }
        }
        if ($rt.TargetEntityType) {
            $record.targetEntity = if ($rt.TargetEntityType.Identifier) { $rt.TargetEntityType.Identifier } else { $rt.TargetEntityType.Id }
        }
    }

    if ($Order.Owner) {
        $o = $Order.Owner
        $record.ownerName = if ($o.InternalDisplayName) { $o.InternalDisplayName }
                            elseif ($o.DisplayName)     { $o.DisplayName }
                            elseif ($o.LastFirstName)   { $o.LastFirstName }
                            else                        { $o.Id }
        $record.ownerIdentifier = $o.Identifier
        $ownerHash = [ordered]@{}
        $o.PSObject.Properties | ForEach-Object { $ownerHash[$_.Name] = $_.Value }
        $record.owner = $ownerHash
    }

    if ($Order.Resource) {
        $r = $Order.Resource
        $record.resourceDisplayName = if ($r.InternalDisplayName) { $r.InternalDisplayName }
                                      elseif ($r.DisplayName)     { $r.DisplayName }
                                      elseif ($r.Identifier)      { $r.Identifier }
                                      else                        { $r.Id }
        $record.resourceIdentifier = $r.Identifier
        $resourceHash = [ordered]@{}
        $r.PSObject.Properties | ForEach-Object { $resourceHash[$_.Name] = $_.Value }
        $record.resource = $resourceHash
    }

    if ($Order.Changes) {
        $changesHash = [ordered]@{}
        $rolesArr    = [System.Collections.ArrayList]::new()
        $Order.Changes.PSObject.Properties | ForEach-Object {
            $propName = $_.Name
            $propVal  = $_.Value
            $isArray  = $propVal -is [System.Collections.IEnumerable] -and -not ($propVal -is [string])
            if ($isArray) {
                $direction = 'unknown'
                if ($propName -match '_add$')         { $direction = 'add'    }
                elseif ($propName -match '_remove$')  { $direction = 'remove' }
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

    return $record
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

# ═══════════════════════════════════════════════════════════════════════════════
# SERVE MODE — functions (only used when -Serve is passed)
# ═══════════════════════════════════════════════════════════════════════════════

# One-pass index build: scans all files once to collect metadata and file paths.
# Does NOT store full records — memory footprint stays tiny regardless of dataset size.
# Returns a hashtable with:
#   FileIndex      - ArrayList of {Path, FileName, IsoDate, ResourceTypeDir, TargetEntity, SourceEntity}
#   Meta           - entity lists, attr lists, totalOrders (for /api/meta)
#   OwnerFileIndex - hashtable: ownerKey -> ArrayList of file paths (for lifecycle)
function Build-ServeIndex {
    param([object[]]$JsonFiles)

    $fileIndex         = [System.Collections.ArrayList]::new()
    $ownerFileIndex    = @{}   # ownerKey (ident or display name) -> ArrayList of paths
    $seSet             = @{}; $teSet = @{}
    $tlMap             = @{}  # date -> {added,modified,deleted} for fullTimeline
    $s2t               = @{}; $t2s   = @{}
    $ownerAttrsSeen    = @{}
    $resourceAttrsSeen = @{}
    $totalOrders       = 0
    $total             = $JsonFiles.Count
    $done              = 0
    $lastPct           = -1

    foreach ($jsonFile in $JsonFiles) {
        $done++
        $pct = [math]::Floor(($done / $total) * 100)
        if ($pct -ne $lastPct -and ($pct % 5 -eq 0 -or $done -eq $total)) {
            Write-Host "  [$pct%] Indexed $done / $total files" -ForegroundColor Gray
            $lastPct = $pct
        }

        $filePath = $jsonFile.FullName
        $fileName = $jsonFile.Name
        $rtDir    = $jsonFile.Directory.Name
        $isoDate  = $null

        if ($fileName -match '^(\d{14})') {
            try { $isoDate = [datetime]::ParseExact($Matches[1], 'yyyyMMddHHmmss', $null).ToString('yyyy-MM-ddTHH:mm:ss') }
            catch { continue }
        } else { continue }

        $fileInfo = [ordered]@{
            Path            = $filePath
            FileName        = $fileName
            IsoDate         = $isoDate
            ResourceTypeDir = $rtDir
            TargetEntity    = $null
            SourceEntity    = $null
        }

        try {
            $content = Get-Content -Path $filePath -Raw -Encoding UTF8
            $json    = $content | ConvertFrom-Json
        } catch {
            [void]$fileIndex.Add($fileInfo)
            continue
        }

        $orders = if ($json.ProvisioningOrdersList) { @($json.ProvisioningOrdersList) } else { @($json) }

        foreach ($order in $orders) {
            $totalOrders++

            # Timeline accumulation
            $dk = $isoDate.Substring(0, 10)
            if (-not $tlMap.ContainsKey($dk)) { $tlMap[$dk] = @{ added = 0; modified = 0; deleted = 0 } }
            $ct = if ($order.ChangeType) { $order.ChangeType.ToLower() } else { '' }
            if      ($ct -eq 'added')    { $tlMap[$dk].added++ }
            elseif  ($ct -eq 'modified') { $tlMap[$dk].modified++ }
            elseif  ($ct -eq 'deleted')  { $tlMap[$dk].deleted++ }

            # Entity info
            $se = 'Unknown'; $te = 'Unknown'
            if ($order.ResourceType) {
                if ($order.ResourceType.SourceEntityType -and $order.ResourceType.SourceEntityType.Identifier) {
                    $se = $order.ResourceType.SourceEntityType.Identifier
                }
                if ($order.ResourceType.TargetEntityType -and $order.ResourceType.TargetEntityType.Identifier) {
                    $te = $order.ResourceType.TargetEntityType.Identifier
                }
            }
            # Use first order's entities to represent the file (all orders in a folder share the same RT)
            if (-not $fileInfo.TargetEntity) { $fileInfo.TargetEntity = $te; $fileInfo.SourceEntity = $se }
            $seSet[$se] = 1; $teSet[$te] = 1
            if (-not $s2t.ContainsKey($se)) { $s2t[$se] = @{} }
            if (-not $t2s.ContainsKey($te)) { $t2s[$te] = @{} }
            $s2t[$se][$te] = $true; $t2s[$te][$se] = $true

            # Attr keys for filter dropdowns
            if ($order.Owner)    { $order.Owner.PSObject.Properties    | ForEach-Object { $ownerAttrsSeen[$_.Name]    = 1 } }
            if ($order.Resource) { $order.Resource.PSObject.Properties | ForEach-Object { $resourceAttrsSeen[$_.Name] = 1 } }

            # Owner file index: index by both identifier and display name so searches hit either
            if ($order.Owner) {
                $o         = $order.Owner
                $ownerKeys = @()
                if ($o.Identifier) { $ownerKeys += $o.Identifier }
                $disp = if ($o.InternalDisplayName) { $o.InternalDisplayName }
                        elseif ($o.DisplayName)     { $o.DisplayName }
                        elseif ($o.LastFirstName)   { $o.LastFirstName }
                        else                        { $null }
                if ($disp -and $disp -ne $o.Identifier) { $ownerKeys += $disp }
                foreach ($key in $ownerKeys) {
                    if (-not $ownerFileIndex.ContainsKey($key)) {
                        $ownerFileIndex[$key] = [System.Collections.ArrayList]::new()
                    }
                    if (-not $ownerFileIndex[$key].Contains($filePath)) {
                        [void]$ownerFileIndex[$key].Add($filePath)
                    }
                }
            }
        }

        [void]$fileIndex.Add($fileInfo)
    }

    # Build link maps
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

    $fullTimeline = @($tlMap.Keys | Sort-Object | ForEach-Object {
        $e = $tlMap[$_]
        [ordered]@{ date = $_; added = $e.added; modified = $e.modified; deleted = $e.deleted; total = $e.added + $e.modified + $e.deleted }
    })

    $meta = [ordered]@{
        sourceEntities = @($seSet.Keys | Sort-Object)
        targetEntities = @($teSet.Keys | Sort-Object)
        entityLinks    = [ordered]@{ s2t = $s2tOut; t2s = $t2sOut }
        totalOrders    = $totalOrders
        ownerAttrs     = @($ownerAttrsSeen.Keys | Sort-Object)
        resourceAttrs  = @($resourceAttrsSeen.Keys | Sort-Object)
        fullTimeline   = $fullTimeline
    }

    return @{
        FileIndex      = $fileIndex
        Meta           = $meta
        OwnerFileIndex = $ownerFileIndex
    }
}

# Per-query: pre-filters the file index by date/entity, then streams through
# relevant files, parsing and filtering records on the fly.
# Memory = O(matched records for this query), not O(all records).
function Invoke-OrderQuery {
    param([System.Collections.ArrayList]$FileIndex, $QueryString)

    # ── Parse query params ──────────────────────────────────────────────────
    $rawPage  = $QueryString['page'];     $pageNum  = if ($rawPage  -match '^\d+$') { [int]$rawPage  } else { 0 }
    $rawSize  = $QueryString['pageSize']; $pageSize = if ($rawSize  -match '^\d+$') { [int]$rawSize  } else { 50 }
    if ($pageSize -lt 1 -or $pageSize -gt 500) { $pageSize = 50 }

    $sortCol  = $QueryString['sortCol'];  if (-not $sortCol)  { $sortCol  = 'fileDate' }
    $sortDir  = $QueryString['sortDir'];  if ($sortDir -ne 'asc') { $sortDir = 'desc' }

    $seFilter        = $QueryString['sourceEntity']
    $teFilter        = $QueryString['targetEntity']
    $search          = $QueryString['search']
    $ownerName       = $QueryString['ownerName']
    $ownerIdent      = $QueryString['ownerIdentifier']
    $ownerAttr       = $QueryString['ownerAttr']
    $ownerAttrValue  = $QueryString['ownerAttrValue']
    $resName         = $QueryString['resourceName']
    $resIdent        = $QueryString['resourceIdentifier']
    $resAttr         = $QueryString['resourceAttr']
    $resAttrValue    = $QueryString['resourceAttrValue']
    $dateFrom        = $QueryString['dateFrom']
    $dateTo          = $QueryString['dateTo']
    $chMin           = $QueryString['changesMin']
    $chMax           = $QueryString['changesMax']
    $chAttr          = $QueryString['changesAttr']
    $chVal           = $QueryString['changesValue']
    $rlMin           = $QueryString['rolesMin']
    $rlMax           = $QueryString['rolesMax']
    $rlDir           = $QueryString['rolesDir']
    $rlVal           = $QueryString['rolesValue']

    $ctRaw = $QueryString['changeTypes']
    $ctOn  = @{ Added = $true; Modified = $true; Deleted = $true }
    if ($ctRaw) {
        $ctOn = @{ Added = $false; Modified = $false; Deleted = $false }
        $ctRaw -split ',' | ForEach-Object { $ctOn[$_.Trim()] = $true }
    }

    # ── Pre-filter files — skips entire files that can't possibly match ─────
    $relevantFiles = @($FileIndex | Where-Object {
        $f = $_
        if ($dateFrom -and $f.IsoDate -lt $dateFrom) { return $false }
        if ($dateTo   -and $f.IsoDate -gt ($dateTo + 'T23:59:59')) { return $false }
        if ($teFilter -and $teFilter -ne '__all__' -and $f.TargetEntity -and $f.TargetEntity -ne $teFilter) { return $false }
        if ($seFilter -and $seFilter -ne '__all__' -and $f.SourceEntity -and $f.SourceEntity -ne $seFilter) { return $false }
        return $true
    })

    # ── Stream through files, parse and filter records on the fly ──────────
    $filtered = [System.Collections.ArrayList]::new()

    foreach ($fileInfo in $relevantFiles) {
        try {
            $content = Get-Content -Path $fileInfo.Path -Raw -Encoding UTF8
            $json    = $content | ConvertFrom-Json
        } catch { continue }

        $orders = if ($json.ProvisioningOrdersList) { @($json.ProvisioningOrdersList) } else { @($json) }

        foreach ($order in $orders) {
            $d = New-OrderRecord -Order $order -FileName $fileInfo.FileName -IsoDate $fileInfo.IsoDate -ResourceTypeDir $fileInfo.ResourceTypeDir

            # Entity
            if ($seFilter -and $seFilter -ne '__all__') {
                if ((if ($d.sourceEntity) { $d.sourceEntity } else { 'Unknown' }) -ne $seFilter) { continue }
            }
            if ($teFilter -and $teFilter -ne '__all__') {
                if ((if ($d.targetEntity) { $d.targetEntity } else { 'Unknown' }) -ne $teFilter) { continue }
            }

            # Change type
            $ct   = if ($d.changeType) { $d.changeType } else { '' }
            $norm = if ($ct.Length -gt 0) { $ct.Substring(0,1).ToUpper() + $ct.Substring(1).ToLower() } else { '' }
            if (-not $ctOn[$norm]) { continue }

            # Search
            if ($search) {
                $q   = $search.ToLower()
                $hay = @($d.ownerName, $d.ownerIdentifier, $d.fileName, $d.sourceEntity,
                         $d.targetEntity, $d.resourceType, $d.resourceTypeDir) | Where-Object { $_ -ne $null }
                if (($hay -join ' ').ToLower().IndexOf($q) -lt 0) { continue }
            }

            # Owner
            if ($ownerName  -and ($null -eq $d.ownerName       -or $d.ownerName.ToLower().IndexOf($ownerName.ToLower())       -lt 0)) { continue }
            if ($ownerIdent -and ($null -eq $d.ownerIdentifier  -or $d.ownerIdentifier.ToLower().IndexOf($ownerIdent.ToLower()) -lt 0)) { continue }
            if ($ownerAttr -and $ownerAttr -ne '__all__') {
                $oo = $d.owner
                if ($null -eq $oo -or -not $oo.Contains($ownerAttr)) { continue }
                if ($ownerAttrValue -and $ownerAttrValue -ne '') {
                    $v = $oo[$ownerAttr]
                    if ($null -eq $v -or $v.ToString().ToLower().IndexOf($ownerAttrValue.ToLower()) -lt 0) { continue }
                }
            } elseif ($ownerAttrValue -and $ownerAttrValue -ne '') {
                $oo = $d.owner; $oFound = $false; $oq = $ownerAttrValue.ToLower()
                if ($null -ne $oo) {
                    foreach ($k in $oo.Keys) {
                        $v = $oo[$k]
                        if ($null -ne $v -and $v.ToString().ToLower().IndexOf($oq) -ge 0) { $oFound = $true; break }
                    }
                }
                if (-not $oFound) { continue }
            }

            # Resource
            if ($resName  -and ($null -eq $d.resourceDisplayName -or $d.resourceDisplayName.ToLower().IndexOf($resName.ToLower())   -lt 0)) { continue }
            if ($resIdent -and ($null -eq $d.resourceIdentifier  -or $d.resourceIdentifier.ToLower().IndexOf($resIdent.ToLower())   -lt 0)) { continue }
            if ($resAttr -and $resAttr -ne '__all__') {
                $ro = $d.resource
                if ($null -eq $ro -or -not $ro.Contains($resAttr)) { continue }
                if ($resAttrValue -and $resAttrValue -ne '') {
                    $v = $ro[$resAttr]
                    if ($null -eq $v -or $v.ToString().ToLower().IndexOf($resAttrValue.ToLower()) -lt 0) { continue }
                }
            } elseif ($resAttrValue -and $resAttrValue -ne '') {
                $ro = $d.resource; $rFound = $false; $rq = $resAttrValue.ToLower()
                if ($null -ne $ro) {
                    foreach ($k in $ro.Keys) {
                        $v = $ro[$k]
                        if ($null -ne $v -and $v.ToString().ToLower().IndexOf($rq) -ge 0) { $rFound = $true; break }
                    }
                }
                if (-not $rFound) { continue }
            }

            # Changes
            $nch = if ($d.changes) { $d.changes.Count } else { 0 }
            if ($chMin -and $chMin -ne '' -and $nch -lt [int]$chMin) { continue }
            if ($chMax -and $chMax -ne '' -and $nch -gt [int]$chMax) { continue }
            if ($chAttr -and $chAttr -ne '__all__') {
                if (-not ($d.changes -and $d.changes.Contains($chAttr))) { continue }
            }
            if ($chVal -and $chVal -ne '') {
                $q = $chVal.ToLower(); $found = $false
                if ($d.changes) {
                    if ($chAttr -and $chAttr -ne '__all__') {
                        if ($d.changes.Contains($chAttr)) {
                            $v = $d.changes[$chAttr]
                            $vStr = if ($null -eq $v) { 'null' } else { $v.ToString() }
                            if ($vStr.ToLower().IndexOf($q) -ge 0) { $found = $true }
                        }
                    } else {
                        foreach ($k in $d.changes.Keys) {
                            $v = $d.changes[$k]
                            $vStr = if ($null -eq $v) { 'null' } else { $v.ToString() }
                            if ($vStr.ToLower().IndexOf($q) -ge 0) { $found = $true; break }
                        }
                    }
                }
                if (-not $found) { continue }
            }

            # Roles
            $roles = if ($d.roles) { @($d.roles) } else { @() }
            $nrl   = if ($rlDir -and $rlDir -ne '__all__') { ($roles | Where-Object { $_.direction -eq $rlDir }).Count } else { $roles.Count }
            if ($rlDir -and $rlDir -ne '__all__' -and $nrl -eq 0) { continue }
            if ($rlMin -and $rlMin -ne '' -and $nrl -lt [int]$rlMin) { continue }
            if ($rlMax -and $rlMax -ne '' -and $nrl -gt [int]$rlMax) { continue }
            if ($rlVal -and $rlVal -ne '') {
                $rq = $rlVal.ToLower(); $rlFound = $false
                foreach ($r in $roles) {
                    if ($rlDir -and $rlDir -ne '__all__' -and $r.direction -ne $rlDir) { continue }
                    if ($r.name -and $r.name.ToLower().IndexOf($rq) -ge 0) { $rlFound = $true; break }
                }
                if (-not $rlFound) { continue }
            }

            [void]$filtered.Add($d)
        }
    }

    # ── Stats & timeline from the full matched set ──────────────────────────
    $stats    = Get-OrderStats    -Orders $filtered
    $timeline = Get-OrderTimeline -Orders $filtered

    # ── Available attrs for current target entity filter ────────────────────
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
        default {
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

# Lifecycle: uses the OwnerFileIndex to read only files containing matching owners.
# No full dataset scan needed — just the files relevant to the search query.
function Get-LifecycleData {
    param($OwnerFileIndex, [string]$Search)

    if (-not $Search -or $Search.Length -lt 2) {
        return [ordered]@{ owners = @(); total = 0; truncated = $false }
    }

    $q = $Search.ToLower()

    # Find all owner keys (identifiers + display names) that match the query
    $matchingKeys = @($OwnerFileIndex.Keys | Where-Object { $_.ToLower().IndexOf($q) -ge 0 })

    # Collect unique file paths for all matching owners
    $filesToRead = [System.Collections.Generic.HashSet[string]]::new()
    foreach ($key in $matchingKeys) {
        foreach ($path in $OwnerFileIndex[$key]) { [void]$filesToRead.Add($path) }
    }

    $owners = [ordered]@{}

    foreach ($filePath in $filesToRead) {
        $fileName = [System.IO.Path]::GetFileName($filePath)
        $rtDir    = [System.IO.Path]::GetFileName([System.IO.Path]::GetDirectoryName($filePath))
        $isoDate  = $null

        if ($fileName -match '^(\d{14})') {
            try { $isoDate = [datetime]::ParseExact($Matches[1], 'yyyyMMddHHmmss', $null).ToString('yyyy-MM-ddTHH:mm:ss') }
            catch { continue }
        } else { continue }

        try {
            $content = Get-Content -Path $filePath -Raw -Encoding UTF8
            $json    = $content | ConvertFrom-Json
        } catch { continue }

        $orders = if ($json.ProvisioningOrdersList) { @($json.ProvisioningOrdersList) } else { @($json) }

        foreach ($order in $orders) {
            $d    = New-OrderRecord -Order $order -FileName $fileName -IsoDate $isoDate -ResourceTypeDir $rtDir
            $name = if ($d.ownerName)       { $d.ownerName }       else { 'Unknown' }
            $id   = if ($d.ownerIdentifier) { $d.ownerIdentifier } else { '' }

            # Verify this record's owner actually matches the search (batch files can contain multiple owners)
            if ($name.ToLower().IndexOf($q) -lt 0 -and $id.ToLower().IndexOf($q) -lt 0) { continue }

            $key = if ($d.ownerIdentifier) { $d.ownerIdentifier } else { $name }
            if (-not $owners.Contains($key)) {
                $owners[$key] = [ordered]@{ name = $name; id = $id; orders = [System.Collections.ArrayList]::new() }
            }
            [void]$owners[$key].orders.Add($d)
        }
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
    param($ServeIndex, [string]$HtmlTemplate, [int]$Port)

    $ErrorActionPreference = 'Continue'

    $serveHtml = $HtmlTemplate.Replace(
        'window.__RAW_DATA__ = __DATA_PLACEHOLDER__;',
        "window.__RAW_DATA__ = null; window.__API_BASE__ = 'http://localhost:$Port';"
    )
    $serveHtmlBytes = [System.Text.Encoding]::UTF8.GetBytes($serveHtml)

    $metaJson  = $ServeIndex.Meta | ConvertTo-Json -Depth 10 -Compress
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
    Write-Host "  Files indexed : $($ServeIndex.FileIndex.Count)" -ForegroundColor Cyan
    Write-Host "  Total orders  : $($ServeIndex.Meta.totalOrders)" -ForegroundColor Cyan
    Write-Host "  Press Ctrl+C to stop." -ForegroundColor Gray
    Write-Host ""

    Start-Process $url

    try {
        while ($listener.IsListening) {
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
                        $result = Invoke-OrderQuery -FileIndex $ServeIndex.FileIndex -QueryString $qs
                        $json   = $result | ConvertTo-Json -Depth 10 -Compress
                        Send-JsonResponse -Response $resp -Json $json
                        Write-Verbose "  <-- 200 query ($($result.total) matched, page $($result.page))"
                    }
                    '^/api/lifecycle$' {
                        $qs     = ConvertFrom-QueryString $req.Url.Query
                        $result = Get-LifecycleData -OwnerFileIndex $ServeIndex.OwnerFileIndex -Search $qs['search']
                        $json   = $result | ConvertTo-Json -Depth 10 -Compress
                        Send-JsonResponse -Response $resp -Json $json
                        Write-Verbose "  <-- 200 lifecycle ($($result.total) owners)"
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
    Write-Host "    -BasePath [path]      Root folder containing provisioning orders."
    Write-Host "                          Default: C:\Usercube\Work\ProvisioningOrders"
    Write-Host ""
    Write-Host "    -StartDate [date]     Only include orders from this date onward."
    Write-Host "                          Format: yyyy-MM-dd (e.g. 2026-03-01)"
    Write-Host ""
    Write-Host "    -EndDate [date]       Only include orders up to this date."
    Write-Host "                          Format: yyyy-MM-dd (e.g. 2026-03-31)"
    Write-Host ""
    Write-Host "    -Serve                Start a local HTTP server instead of generating a file."
    Write-Host "                          Records are never fully loaded into memory — files are"
    Write-Host "                          streamed and filtered per query. Best for large datasets."
    Write-Host "                          Opens browser automatically. Press Ctrl+C to stop."
    Write-Host ""
    Write-Host "    -Port [number]        Port for the HTTP server (default: 5000)."
    Write-Host "                          Only used with -Serve."
    Write-Host ""
    Write-Host "    -Verbose              Print each HTTP request/response to the console."
    Write-Host "                          Only used with -Serve."
    Write-Host ""
    Write-Host "    -Help (-h)            Show this help message."
    Write-Host ""
    Write-Host "  EXAMPLES:" -ForegroundColor Yellow
    Write-Host "    .\LogViewer.ps1                                              # All orders, generate HTML"
    Write-Host "    .\LogViewer.ps1 -StartDate 2026-03-01 -EndDate 2026-03-31    # March only, generate HTML"
    Write-Host "    .\LogViewer.ps1 -Serve                                       # All orders, local server"
    Write-Host "    .\LogViewer.ps1 -Serve -StartDate 2026-01-01 -Port 8080      # Server on custom port"
    Write-Host ""
    Write-Host "  SERVE vs FILE mode:" -ForegroundColor Yellow
    Write-Host "    File mode  — loads all records into memory and embeds them in a single HTML file."
    Write-Host "                 Practical up to ~80k orders."
    Write-Host "    Serve mode — builds a lightweight file index at startup (tiny memory footprint)."
    Write-Host "                 Each query streams and filters only the relevant files on disk."
    Write-Host "                 No upper limit on dataset size; add date/entity filters for speed."
    Write-Host ""
    Write-Host "  OUTPUT:" -ForegroundColor Yellow
    Write-Host "    Default: generates LogViewer.html next to this script."
    Write-Host "    -Serve:  starts http://localhost:5000, opens browser, stays alive until Ctrl+C."
    Write-Host ""
    return
}

# ═══════════════════════════════════════════════════════════════════════════════
# DISCOVERY — find JSON files (shared between file and serve modes)
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

Write-Host "Found $($allJsonFiles.Count) JSON file(s)." -ForegroundColor Cyan

$templatePath = Join-Path $PSScriptRoot "LogViewer_base.html"
if (-not (Test-Path $templatePath)) {
    Write-Error "LogViewer_base.html not found next to the script at $PSScriptRoot"
    return
}
$htmlTemplate = Get-Content -Path $templatePath -Raw -Encoding UTF8

# ═══════════════════════════════════════════════════════════════════════════════
# OUTPUT — serve mode (streaming) or file mode (full load)
# ═══════════════════════════════════════════════════════════════════════════════

if ($Serve) {
    # Serve mode: build a lightweight file index — no records held in memory.
    # Queries stream and filter files on demand, so memory stays low even for GB datasets.
    Write-Host "Building index (records will be streamed per query)..." -ForegroundColor Cyan
    $serveIndex = Build-ServeIndex -JsonFiles $allJsonFiles
    Write-Host "Index complete: $($serveIndex.FileIndex.Count) files, $($serveIndex.Meta.totalOrders) orders." -ForegroundColor Green
    Start-LogViewerServer -ServeIndex $serveIndex -HtmlTemplate $htmlTemplate -Port $Port
} else {
    # File mode: load all records into memory and embed them in a self-contained HTML file.
    if ($allJsonFiles.Count -gt 80000) {
        Write-Warning "$($allJsonFiles.Count) files exceeds the recommended limit (~80k) for file mode."
        Write-Warning "The generated HTML may be slow or fail to open. Consider using -Serve instead."
    }

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
                $isoDate = [datetime]::ParseExact($Matches[1], 'yyyyMMddHHmmss', $null).ToString('yyyy-MM-ddTHH:mm:ss')
            } catch { continue }
        } else { continue }

        try {
            $content = Get-Content -Path $jsonFile.FullName -Raw -Encoding UTF8
            $json    = $content | ConvertFrom-Json
        } catch { continue }

        $orders = if ($json.ProvisioningOrdersList) { @($json.ProvisioningOrdersList) } else { @($json) }

        foreach ($order in $orders) {
            $record = New-OrderRecord -Order $order -FileName $jsonFile.Name -IsoDate $isoDate -ResourceTypeDir $jsonFile.Directory.Name
            [void]$allOrders.Add($record)
        }
    }

    if ($allOrders.Count -eq 0) {
        Write-Warning "No provisioning orders found. No output generated."
        return
    }

    Write-Host "Loaded $($allOrders.Count) provisioning orders." -ForegroundColor Green
    Write-Host "Serializing JSON..." -ForegroundColor Gray
    $jsonData = $allOrders | ConvertTo-Json -Depth 10 -Compress

    Write-Host "Writing HTML..." -ForegroundColor Gray
    $htmlFinal = $htmlTemplate.Replace('__DATA_PLACEHOLDER__', $jsonData)
    [System.IO.File]::WriteAllText($OutputPath, $htmlFinal, [System.Text.Encoding]::UTF8)

    Write-Host "`nDone: $OutputPath" -ForegroundColor Green
    Write-Host "Orders: $($allOrders.Count)" -ForegroundColor Cyan
    Start-Process $OutputPath
}
