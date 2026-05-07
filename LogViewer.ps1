[CmdletBinding()]
param(
    [string]$BasePath = "C:\Usercube_Root\Usercube_Server\Work\ProvisioningOrders",
    [DateTime]$StartDate,
    [DateTime]$EndDate,
    [switch]$Serve,
    [int]$Port = 5005,
    [string]$SourceEntityType = "Directory_User",
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

# One-pass index build: scans all files once, stores a lightweight filterable
# record per order (flat fields for fast filtering) plus the full JSON string
# (deserialized only for the records in the current page).
# Memory: ~300-500MB for 200k orders (mostly JSON strings).
function Build-ServeIndex {
    param([object[]]$JsonFiles)

    # OrderIndex: ArrayList of lightweight hashtables for fast filtering
    # Each entry: { idx, fileDate, dateKey, changeType, changeTypeNorm,
    #               sourceEntity, targetEntity, ownerName, ownerNameLower,
    #               ownerIdentifier, ownerIdentLower, resourceDisplayName,
    #               resourceIdentifier, changesCount, rolesCount,
    #               rolesAddCount, rolesRemoveCount, changesKeys, searchHay }
    $orderIndex   = [System.Collections.ArrayList]::new()
    # Full records stored as JSON strings — only deserialized for page results
    $orderJsons   = [System.Collections.ArrayList]::new()

    $ownerFileIndex    = @{}
    $seSet = @{}; $teSet = @{}
    $tlMap = @{}
    $s2t   = @{}; $t2s   = @{}
    $ownerAttrsSeen    = @{}
    $resourceAttrsSeen = @{}
    $changeAttrsByTE   = @{}   # targetEntity -> { attrName -> 1 }
    $totalOrders       = 0
    $total = $JsonFiles.Count
    $done  = 0; $lastPct = -1

    foreach ($jsonFile in $JsonFiles) {
        $done++
        $pct = [math]::Floor(($done / $total) * 100)
        if ($pct -ne $lastPct -and ($pct % 5 -eq 0 -or $done -eq $total)) {
            Write-Host "  [$pct%] Indexed $done / $total files ($totalOrders orders)" -ForegroundColor Gray
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

        try {
            $content = [System.IO.File]::ReadAllText($filePath)
            $json    = $content | ConvertFrom-Json
        } catch { continue }

        $orders = if ($json.ProvisioningOrdersList) { @($json.ProvisioningOrdersList) } else { @($json) }

        foreach ($order in $orders) {
            $idx = $totalOrders
            $totalOrders++

            # Build full record and serialize to JSON string
            $fullRecord = New-OrderRecord -Order $order -FileName $fileName -IsoDate $isoDate -ResourceTypeDir $rtDir
            [void]$orderJsons.Add(($fullRecord | ConvertTo-Json -Depth 10 -Compress))

            # Extract lightweight filterable fields
            $ct = if ($order.ChangeType) { $order.ChangeType } else { '' }
            $ctNorm = if ($ct.Length -gt 0) { $ct.Substring(0,1).ToUpper() + $ct.Substring(1).ToLower() } else { '' }

            $se = 'Unknown'; $te = 'Unknown'
            if ($order.ResourceType) {
                if ($order.ResourceType.SourceEntityType -and $order.ResourceType.SourceEntityType.Identifier) {
                    $se = $order.ResourceType.SourceEntityType.Identifier
                }
                if ($order.ResourceType.TargetEntityType -and $order.ResourceType.TargetEntityType.Identifier) {
                    $te = $order.ResourceType.TargetEntityType.Identifier
                }
            }

            $owName = $fullRecord.ownerName
            $owId   = $fullRecord.ownerIdentifier
            $resName = $fullRecord.resourceDisplayName
            $resId   = $fullRecord.resourceIdentifier

            # Changes info
            $chKeys = @()
            $chCount = 0
            if ($fullRecord.changes) {
                $chKeys  = @($fullRecord.changes.Keys)
                $chCount = $chKeys.Count
            }

            # Roles info
            $roles = if ($fullRecord.roles) { @($fullRecord.roles) } else { @() }
            $rlCount = $roles.Count
            $rlAdd   = ($roles | Where-Object { $_.direction -eq 'add' }).Count
            $rlRm    = ($roles | Where-Object { $_.direction -eq 'remove' }).Count

            # Search haystack (precomputed lowercase)
            $hay = @($owName, $owId, $fileName, $se, $te, $fullRecord.resourceType, $rtDir) |
                   Where-Object { $_ } | ForEach-Object { $_.ToLower() }
            $searchHay = $hay -join ' '

            $lightRecord = @{
                idx               = $idx
                fileDate          = $isoDate
                dateKey           = $isoDate.Substring(0, 10)
                changeType        = $ct
                changeTypeNorm    = $ctNorm
                sourceEntity      = $se
                targetEntity      = $te
                ownerName         = $owName
                ownerNameLower    = if ($owName) { $owName.ToLower() } else { '' }
                ownerIdentifier   = $owId
                ownerIdentLower   = if ($owId) { $owId.ToLower() } else { '' }
                resourceName      = $resName
                resourceNameLower = if ($resName) { $resName.ToLower() } else { '' }
                resourceIdent     = $resId
                resourceIdentLower= if ($resId) { $resId.ToLower() } else { '' }
                changesCount      = $chCount
                changesKeys       = $chKeys
                rolesCount        = $rlCount
                rolesAddCount     = $rlAdd
                rolesRemoveCount  = $rlRm
                searchHay         = $searchHay
            }
            [void]$orderIndex.Add($lightRecord)

            # Timeline
            $dk = $isoDate.Substring(0, 10)
            if (-not $tlMap.ContainsKey($dk)) { $tlMap[$dk] = @{ added = 0; modified = 0; deleted = 0 } }
            $ctL = $ct.ToLower()
            if     ($ctL -eq 'added')    { $tlMap[$dk].added++ }
            elseif ($ctL -eq 'modified') { $tlMap[$dk].modified++ }
            elseif ($ctL -eq 'deleted')  { $tlMap[$dk].deleted++ }

            # Entity sets and links
            $seSet[$se] = 1; $teSet[$te] = 1
            if (-not $s2t.ContainsKey($se)) { $s2t[$se] = @{} }
            if (-not $t2s.ContainsKey($te)) { $t2s[$te] = @{} }
            $s2t[$se][$te] = $true; $t2s[$te][$se] = $true

            # Change attrs by target entity
            if (-not $changeAttrsByTE.ContainsKey($te)) { $changeAttrsByTE[$te] = @{} }
            foreach ($ck in $chKeys) { $changeAttrsByTE[$te][$ck] = 1 }

            # Attr sets for dropdowns
            if ($order.Owner)    { $order.Owner.PSObject.Properties    | ForEach-Object { $ownerAttrsSeen[$_.Name]    = 1 } }
            if ($order.Resource) { $order.Resource.PSObject.Properties | ForEach-Object { $resourceAttrsSeen[$_.Name] = 1 } }

            # Owner file index for lifecycle
            if ($order.Owner) {
                $o = $order.Owner
                $ownerKeys = @()
                if ($o.Identifier) { $ownerKeys += $o.Identifier }
                $disp = if ($o.InternalDisplayName) { $o.InternalDisplayName }
                        elseif ($o.DisplayName)     { $o.DisplayName }
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
    }

    # Build link maps
    $s2tOut = [ordered]@{}
    foreach ($sk in ($s2t.Keys | Sort-Object)) {
        $s2tOut[$sk] = [ordered]@{}; foreach ($tk in $s2t[$sk].Keys) { $s2tOut[$sk][$tk] = $true }
    }
    $t2sOut = [ordered]@{}
    foreach ($tk in ($t2s.Keys | Sort-Object)) {
        $t2sOut[$tk] = [ordered]@{}; foreach ($sk in $t2s[$tk].Keys) { $t2sOut[$tk][$sk] = $true }
    }

    # Build change attrs by TE for meta
    $changeAttrsByTEOut = [ordered]@{}
    foreach ($tk in ($changeAttrsByTE.Keys | Sort-Object)) {
        $changeAttrsByTEOut[$tk] = @($changeAttrsByTE[$tk].Keys | Sort-Object)
    }

    $fullTimeline = @($tlMap.Keys | Sort-Object | ForEach-Object {
        $e = $tlMap[$_]
        [ordered]@{ date = $_; added = $e.added; modified = $e.modified; deleted = $e.deleted; total = $e.added + $e.modified + $e.deleted }
    })

    # Pre-sort OrderIndex by fileDate descending — this is the default sort,
    # so unfiltered queries (tab switches, initial load) skip the runtime sort entirely.
    Write-Host "  Pre-sorting index by fileDate..." -ForegroundColor Gray
    $sortedIndex = [System.Collections.ArrayList]::new()
    $orderIndex | Sort-Object -Property fileDate -Descending | ForEach-Object { [void]$sortedIndex.Add($_) }
    $orderIndex = $sortedIndex

    $meta = [ordered]@{
        sourceEntities    = @($seSet.Keys | Sort-Object)
        targetEntities    = @($teSet.Keys | Sort-Object)
        entityLinks       = [ordered]@{ s2t = $s2tOut; t2s = $t2sOut }
        totalOrders       = $totalOrders
        ownerAttrs        = @($ownerAttrsSeen.Keys | Sort-Object)
        resourceAttrs     = @($resourceAttrsSeen.Keys | Sort-Object)
        fullTimeline      = $fullTimeline
        changeAttrsByTE   = $changeAttrsByTEOut
    }

    return @{
        OrderIndex     = $orderIndex
        OrderJsons     = $orderJsons
        Meta           = $meta
        OwnerFileIndex = $ownerFileIndex
    }
}

# Per-query: filters the in-memory lightweight index (no disk I/O),
# computes stats/timeline from matched set, then deserializes only the
# records needed for the current page.
function Invoke-OrderQuery {
    param($ServeIndex, $QueryString)

    $orderIndex = $ServeIndex.OrderIndex
    $orderJsons = $ServeIndex.OrderJsons

    # ── Parse query params ──────────────────────────────────────────────────
    $rawPage  = $QueryString['page'];     $pageNum  = if ($rawPage  -match '^\d+$') { [int]$rawPage  } else { 0 }
    $rawSize  = $QueryString['pageSize']; $pageSize = if ($rawSize  -match '^\d+$') { [int]$rawSize  } else { 50 }
    if ($pageSize -lt 1 -or $pageSize -gt 500) { $pageSize = 50 }

    $sortCol  = $QueryString['sortCol'];  if (-not $sortCol)  { $sortCol  = 'fileDate' }
    $sortDir  = $QueryString['sortDir'];  if ($sortDir -ne 'asc') { $sortDir = 'desc' }

    $seFilter        = $QueryString['sourceEntity']
    $teFilter        = $QueryString['targetEntity']
    $search          = if ($QueryString['search']) { $QueryString['search'].ToLower() } else { $null }
    $ownerName       = if ($QueryString['ownerName']) { $QueryString['ownerName'].ToLower() } else { $null }
    $ownerIdent      = if ($QueryString['ownerIdentifier']) { $QueryString['ownerIdentifier'].ToLower() } else { $null }
    $ownerAttr       = $QueryString['ownerAttr']
    $ownerAttrValue  = if ($QueryString['ownerAttrValue']) { $QueryString['ownerAttrValue'].ToLower() } else { $null }
    $resName         = if ($QueryString['resourceName']) { $QueryString['resourceName'].ToLower() } else { $null }
    $resIdent        = if ($QueryString['resourceIdentifier']) { $QueryString['resourceIdentifier'].ToLower() } else { $null }
    $resAttr         = $QueryString['resourceAttr']
    $resAttrValue    = if ($QueryString['resourceAttrValue']) { $QueryString['resourceAttrValue'].ToLower() } else { $null }
    $dateFrom        = $QueryString['dateFrom']
    $dateTo          = $QueryString['dateTo']
    $chMin           = $QueryString['changesMin']; $chMinI = if ($chMin -match '^\d+$') { [int]$chMin } else { -1 }
    $chMax           = $QueryString['changesMax']; $chMaxI = if ($chMax -match '^\d+$') { [int]$chMax } else { -1 }
    $chAttr          = $QueryString['changesAttr']
    $chVal           = if ($QueryString['changesValue']) { $QueryString['changesValue'].ToLower() } else { $null }
    $rlMin           = $QueryString['rolesMin']; $rlMinI = if ($rlMin -match '^\d+$') { [int]$rlMin } else { -1 }
    $rlMax           = $QueryString['rolesMax']; $rlMaxI = if ($rlMax -match '^\d+$') { [int]$rlMax } else { -1 }
    $rlDir           = $QueryString['rolesDir']
    $rlVal           = if ($QueryString['rolesValue']) { $QueryString['rolesValue'].ToLower() } else { $null }

    $ctRaw = $QueryString['changeTypes']
    $ctOn  = @{ Added = $true; Modified = $true; Deleted = $true }
    if ($ctRaw) {
        $ctOn = @{ Added = $false; Modified = $false; Deleted = $false }
        $ctRaw -split ',' | ForEach-Object { $ctOn[$_.Trim()] = $true }
    }

    # Need deep filtering? (requires deserializing full records)
    $needDeep = ($ownerAttr -and $ownerAttr -ne '__all__') -or $ownerAttrValue -or
                ($resAttr -and $resAttr -ne '__all__') -or $resAttrValue -or
                $chVal -or $rlVal

    # ── Fast filter on lightweight index ────────────────────────────────────
    $matchedIndices = [System.Collections.ArrayList]::new()

    foreach ($r in $orderIndex) {
        # Date
        if ($dateFrom -and $r.fileDate -lt $dateFrom) { continue }
        if ($dateTo   -and $r.fileDate -gt ($dateTo + 'T23:59:59')) { continue }
        # Entity
        if ($seFilter -and $seFilter -ne '__all__' -and $r.sourceEntity -ne $seFilter) { continue }
        if ($teFilter -and $teFilter -ne '__all__' -and $r.targetEntity -ne $teFilter) { continue }
        # Change type
        if (-not $ctOn[$r.changeTypeNorm]) { continue }
        # Search
        if ($search -and $r.searchHay.IndexOf($search) -lt 0) { continue }
        # Owner name/ident (fast, precomputed lowercase)
        if ($ownerName  -and $r.ownerNameLower.IndexOf($ownerName)   -lt 0) { continue }
        if ($ownerIdent -and $r.ownerIdentLower.IndexOf($ownerIdent) -lt 0) { continue }
        # Resource name/ident
        if ($resName  -and $r.resourceNameLower.IndexOf($resName)    -lt 0) { continue }
        if ($resIdent -and $r.resourceIdentLower.IndexOf($resIdent)  -lt 0) { continue }
        # Changes count
        if ($chMinI -ge 0 -and $r.changesCount -lt $chMinI) { continue }
        if ($chMaxI -ge 0 -and $r.changesCount -gt $chMaxI) { continue }
        # Changes attr presence
        if ($chAttr -and $chAttr -ne '__all__' -and $r.changesKeys -notcontains $chAttr) { continue }
        # Roles count (by direction)
        $nrl = if ($rlDir -eq 'add') { $r.rolesAddCount } elseif ($rlDir -eq 'remove') { $r.rolesRemoveCount } else { $r.rolesCount }
        if ($rlDir -and $rlDir -ne '__all__' -and $nrl -eq 0) { continue }
        if ($rlMinI -ge 0 -and $nrl -lt $rlMinI) { continue }
        if ($rlMaxI -ge 0 -and $nrl -gt $rlMaxI) { continue }

        [void]$matchedIndices.Add($r)
    }

    # ── Deep filter (deserialize only if needed) ────────────────────────────
    if ($needDeep) {
        $deepFiltered = [System.Collections.ArrayList]::new()
        foreach ($r in $matchedIndices) {
            $d = $orderJsons[$r.idx] | ConvertFrom-Json

            # Owner attr filter
            if ($ownerAttr -and $ownerAttr -ne '__all__') {
                $oo = $d.owner
                if ($null -eq $oo -or -not ($oo.PSObject.Properties.Name -contains $ownerAttr)) { continue }
                if ($ownerAttrValue) {
                    $v = $oo.$ownerAttr
                    if ($null -eq $v -or $v.ToString().ToLower().IndexOf($ownerAttrValue) -lt 0) { continue }
                }
            } elseif ($ownerAttrValue) {
                $oo = $d.owner; $oFound = $false
                if ($null -ne $oo) {
                    foreach ($p in $oo.PSObject.Properties) {
                        if ($null -ne $p.Value -and $p.Value.ToString().ToLower().IndexOf($ownerAttrValue) -ge 0) { $oFound = $true; break }
                    }
                }
                if (-not $oFound) { continue }
            }

            # Resource attr filter
            if ($resAttr -and $resAttr -ne '__all__') {
                $ro = $d.resource
                if ($null -eq $ro -or -not ($ro.PSObject.Properties.Name -contains $resAttr)) { continue }
                if ($resAttrValue) {
                    $v = $ro.$resAttr
                    if ($null -eq $v -or $v.ToString().ToLower().IndexOf($resAttrValue) -lt 0) { continue }
                }
            } elseif ($resAttrValue) {
                $ro = $d.resource; $rFound = $false
                if ($null -ne $ro) {
                    foreach ($p in $ro.PSObject.Properties) {
                        if ($null -ne $p.Value -and $p.Value.ToString().ToLower().IndexOf($resAttrValue) -ge 0) { $rFound = $true; break }
                    }
                }
                if (-not $rFound) { continue }
            }

            # Changes value filter
            if ($chVal) {
                $found = $false
                if ($d.changes) {
                    if ($chAttr -and $chAttr -ne '__all__') {
                        if ($d.changes.PSObject.Properties.Name -contains $chAttr) {
                            $v = $d.changes.$chAttr
                            $vStr = if ($null -eq $v) { 'null' } else { $v.ToString() }
                            if ($vStr.ToLower().IndexOf($chVal) -ge 0) { $found = $true }
                        }
                    } else {
                        foreach ($p in $d.changes.PSObject.Properties) {
                            $vStr = if ($null -eq $p.Value) { 'null' } else { $p.Value.ToString() }
                            if ($vStr.ToLower().IndexOf($chVal) -ge 0) { $found = $true; break }
                        }
                    }
                }
                if (-not $found) { continue }
            }

            # Roles value filter
            if ($rlVal) {
                $roles = if ($d.roles) { @($d.roles) } else { @() }
                $rlFound = $false
                foreach ($rl in $roles) {
                    if ($rlDir -and $rlDir -ne '__all__' -and $rl.direction -ne $rlDir) { continue }
                    if ($rl.name -and $rl.name.ToLower().IndexOf($rlVal) -ge 0) { $rlFound = $true; break }
                }
                if (-not $rlFound) { continue }
            }

            [void]$deepFiltered.Add($r)
        }
        $matchedIndices = $deepFiltered
    }

    # ── Stats & timeline from matched set ──────────────────────────────────
    $stats = [ordered]@{ total = $matchedIndices.Count; added = 0; modified = 0; deleted = 0; rtCount = 0 }
    $rtSeen = @{}; $tlmLocal = @{}
    foreach ($r in $matchedIndices) {
        $ctL = $r.changeType.ToLower()
        if     ($ctL -eq 'added')    { $stats.added++ }
        elseif ($ctL -eq 'modified') { $stats.modified++ }
        elseif ($ctL -eq 'deleted')  { $stats.deleted++ }
        $rtSeen[$r.sourceEntity + '>' + $r.targetEntity] = 1
        $dk = $r.dateKey
        if (-not $tlmLocal.ContainsKey($dk)) { $tlmLocal[$dk] = @{ added = 0; modified = 0; deleted = 0 } }
        if     ($ctL -eq 'added')    { $tlmLocal[$dk].added++ }
        elseif ($ctL -eq 'modified') { $tlmLocal[$dk].modified++ }
        elseif ($ctL -eq 'deleted')  { $tlmLocal[$dk].deleted++ }
    }
    $stats.rtCount = $rtSeen.Count
    $timeline = @($tlmLocal.Keys | Sort-Object | ForEach-Object {
        $e = $tlmLocal[$_]
        [ordered]@{ date = $_; added = $e.added; modified = $e.modified; deleted = $e.deleted; total = $e.added + $e.modified + $e.deleted }
    })

    # ── Available attrs for current target entity filter ────────────────────
    $availableAttrs = @()
    if ($teFilter -and $teFilter -ne '__all__' -and $ServeIndex.Meta.changeAttrsByTE.Contains($teFilter)) {
        $availableAttrs = $ServeIndex.Meta.changeAttrsByTE[$teFilter]
    }

    # ── Sort on lightweight fields ─────────────────────────────────────────
    # OrderIndex is pre-sorted by fileDate desc, so the default sort needs no runtime work.
    # $matchedIndices preserves that order since we filter via foreach over the sorted index.
    $desc = ($sortDir -eq 'desc')
    if ($sortCol -eq 'fileDate' -and $desc) {
        $sorted = $matchedIndices
    } else {
        $sorted = switch ($sortCol) {
            'fileDate'     { $matchedIndices | Sort-Object -Property fileDate -Descending:$desc }
            'changesCount' { $matchedIndices | Sort-Object -Property changesCount -Descending:$desc }
            'rolesCount'   {
                $matchedIndices | Sort-Object {
                    if ($rlDir -eq 'add') { $_.rolesAddCount } elseif ($rlDir -eq 'remove') { $_.rolesRemoveCount } else { $_.rolesCount }
                } -Descending:$desc
            }
            'ownerName'    { $matchedIndices | Sort-Object -Property ownerNameLower -Descending:$desc }
            default        { $matchedIndices | Sort-Object -Property $sortCol -Descending:$desc }
        }
    }

    # ── Paginate and deserialize only page records ─────────────────────────
    $pageItems = @($sorted) | Select-Object -Skip ($pageNum * $pageSize) -First $pageSize
    $records = @($pageItems | ForEach-Object { $orderJsons[$_.idx] | ConvertFrom-Json })

    return [ordered]@{
        total          = $matchedIndices.Count
        page           = $pageNum
        pageSize       = $pageSize
        records        = $records
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
    Write-Host "  Orders indexed: $($ServeIndex.OrderIndex.Count)" -ForegroundColor Cyan
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
                        $result = Invoke-OrderQuery -ServeIndex $ServeIndex -QueryString $qs
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
                    '^/api/pending$' {
                        Write-Verbose "  --> Fetching pending orders..."
                        try {
                            $pendingGroups = Get-PendingOrders
                            $json = [ordered]@{ groups = @($pendingGroups); total = $pendingGroups.Count } | ConvertTo-Json -Depth 10 -Compress
                            Send-JsonResponse -Response $resp -Json $json
                            Write-Verbose "  <-- 200 pending ($($pendingGroups.Count) groups)"
                        } catch {
                            Write-Warning "  Pending fetch failed: $_"
                            Send-JsonResponse -Response $resp -Json "{`"error`":`"$($_.ToString().Replace('"','\"'))`"}"
                        }
                    }
                    '^/api/pending/detail$' {
                        $qs = ConvertFrom-QueryString $req.Url.Query
                        $ownerId = $qs['ownerId']
                        $artId   = $qs['artId']
                        Write-Verbose "  --> Detail for owner=$ownerId art=$artId"
                        try {
                            $detail = Get-UserARTDetail -OwnerId $ownerId -ARTId $artId
                            $json = $detail | ConvertTo-Json -Depth 10 -Compress
                            Send-JsonResponse -Response $resp -Json $json
                            Write-Verbose "  <-- 200 detail ($($detail.scalars.Count) scalars, $($detail.navigations.Count) navs)"
                        } catch {
                            Write-Warning "  Detail fetch failed: $_"
                            Send-JsonResponse -Response $resp -Json "{`"error`":`"$($_.ToString().Replace('"','\"'))`"}"
                        }
                    }
                    '^/api/cancel/navigation$' {
                        $reader = [System.IO.StreamReader]::new($req.InputStream)
                        $body   = $reader.ReadToEnd(); $reader.Close()
                        $parsed = $body | ConvertFrom-Json
                        Write-Verbose "  --> Cancel navigation: $($parsed.navigationId)"
                        try {
                            $result = Invoke-CancelNavigation -NavigationId $parsed.navigationId
                            Send-JsonResponse -Response $resp -Json ($result | ConvertTo-Json -Compress)
                        } catch {
                            Send-JsonResponse -Response $resp -Json "{`"success`":false,`"message`":`"$($_.ToString().Replace('"','\"'))`"}"
                        }
                    }
                    '^/api/cancel/art$' {
                        $reader = [System.IO.StreamReader]::new($req.InputStream)
                        $body   = $reader.ReadToEnd(); $reader.Close()
                        $parsed = $body | ConvertFrom-Json
                        Write-Verbose "  --> Cancel ART: $($parsed.artId)"
                        try {
                            $result = Invoke-CancelART -ARTId $parsed.artId
                            Send-JsonResponse -Response $resp -Json ($result | ConvertTo-Json -Compress)
                        } catch {
                            Send-JsonResponse -Response $resp -Json "{`"success`":false,`"message`":`"$($_.ToString().Replace('"','\"'))`"}"
                        }
                    }
                    '^/api/cancel/bulk$' {
                        $reader = [System.IO.StreamReader]::new($req.InputStream)
                        $body   = $reader.ReadToEnd(); $reader.Close()
                        $parsed = $body | ConvertFrom-Json
                        $artIds = @($parsed.artIds)
                        Write-Verbose "  --> Bulk cancel: $($artIds.Count) ARTs"
                        $ok = 0; $fail = 0; $errors = [System.Collections.ArrayList]::new()
                        foreach ($aid in $artIds) {
                            try {
                                $null = Invoke-CancelART -ARTId $aid
                                $ok++
                            } catch {
                                $fail++
                                [void]$errors.Add("ART $aid : $($_.ToString())")
                            }
                        }
                        $result = [ordered]@{
                            success  = ($fail -eq 0)
                            message  = "$ok cancelled, $fail failed"
                            total    = $artIds.Count
                            ok       = $ok
                            failed   = $fail
                            errors   = @($errors)
                        }
                        Send-JsonResponse -Response $resp -Json ($result | ConvertTo-Json -Depth 5 -Compress)
                        Write-Verbose "  <-- 200 bulk cancel ($ok ok, $fail fail)"
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
# USERCUBE API — Auth + Pending orders
# ═══════════════════════════════════════════════════════════════════════════════

# Module-scope auth state (refreshed automatically)
$script:UCHeader   = $null
$script:UCAuthDate = $null
$script:UCServerURL  = $null
$script:UCOwnerType  = $null
$script:UCSourceET   = $null

function Request-UsercubeAPI {
    param(
        [ValidateSet("GET","POST","PUT")][string]$Verb,
        [string]$Url,
        [string]$Squery,
        [string]$Body
    )
    if (-not $script:UCHeader) { throw "Not authenticated to Usercube." }

    # Auto-refresh token at 55min
    $elapsed = ((Get-Date) - $script:UCAuthDate).TotalSeconds
    if ($elapsed -gt 3300) {
        $script:UCHeader = Request-UsercubeToken
        $script:UCAuthDate = Get-Date
        Write-Host "  [token refreshed]" -ForegroundColor DarkGray
    }

    $command = $Url + $Squery
    if ($Verb -eq "GET") {
        return Invoke-RestMethod -Uri $command -Method GET -Headers $script:UCHeader -ContentType "application/json"
    } else {
        return Invoke-RestMethod -Uri $command -Method $Verb -Headers $script:UCHeader -Body $Body -ContentType "application/json; charset=utf-8"
    }
}

function Request-UsercubeToken {
    $server = $script:UCServerURL.Split('/')[2].Split(':')[0]
    $tokenBody = @{
        client_id     = "$($script:UCClientId)@$server"
        client_secret = $script:UCClientSecret
        scope         = "usercube_api"
        grant_type    = "client_credentials"
    }
    $login = Invoke-RestMethod -Method POST -Uri "$($script:UCServerURL)/connect/token" -Body $tokenBody
    return @{ Authorization = "Bearer $($login.access_token)" }
}

function Initialize-UsercubeAuth {
    Write-Host ""
    Write-Host "  Usercube API Configuration" -ForegroundColor Cyan
    Write-Host "  ==========================" -ForegroundColor Cyan
    Write-Host ""

    $defaultURL = "http://localhost:5000"
    $urlInput = Read-Host "  Usercube Server URL [$defaultURL]"
    $script:UCServerURL = if ($urlInput.Trim()) { $urlInput.Trim().TrimEnd('/') } else { $defaultURL }

    $defaultClient = "API"
    $clientInput = Read-Host "  Client ID [$defaultClient]"
    $script:UCClientId = if ($clientInput.Trim()) { $clientInput.Trim() } else { $defaultClient }

    $secretInput = Read-Host "  Client Secret" -AsSecureString
    $bstr = [System.Runtime.InteropServices.Marshal]::SecureStringToBSTR($secretInput)
    $script:UCClientSecret = [System.Runtime.InteropServices.Marshal]::PtrToStringAuto($bstr)
    [System.Runtime.InteropServices.Marshal]::ZeroFreeBSTR($bstr)

    Write-Host ""
    Write-Host "  Authenticating..." -ForegroundColor Gray
    $script:UCHeader   = Request-UsercubeToken
    $script:UCAuthDate = Get-Date
    Write-Host "  [+] Authenticated" -ForegroundColor Green

    # Resolve OwnerType (EntityType Id)
    $etUrl   = "$($script:UCServerURL)/api/Metadata/EntityType?api-version=1.0"
    $etQuery = [System.Web.HttpUtility]::UrlEncode("select Id, Identifier where (Identifier = `"$($script:UCSourceET)`")")
    $etPath  = [System.Web.HttpUtility]::UrlEncode("/Connectors/EntityType/Query")
    $etResult = Request-UsercubeAPI -Verb GET -Url $etUrl -Squery "&squery=$etQuery&Path=$etPath&QueryRootEntityType=$([System.Web.HttpUtility]::UrlEncode('EntityType'))"
    $script:UCOwnerType = [int]$etResult.Result.Id
    Write-Host "  [+] OwnerType for $($script:UCSourceET): $($script:UCOwnerType)" -ForegroundColor Green
    Write-Host ""
}

# ── Pending fetch: Scalars + Navigations, grouped by Owner+ART ────────────

function Get-PendingOrders {
    $srvUrl = $script:UCServerURL
    $srcET  = $script:UCSourceET
    $owType = $script:UCOwnerType

    # Fetch pending scalars
    $scalarUrl   = "$srvUrl/api/ProvisioningPolicy/AssignedResourceScalar?api-version=1.0&getCurrentValues=true"
    $scalarQuery = [System.Web.HttpUtility]::UrlEncode("join Owner of type $srcET Owner join Property Property join AssignedResourceType AssignedResourceType join AssignedResourceType.WorkflowInstance AssignedResourceTypeWorkflowInstance join AssignedResourceType.Role AssignedResourceTypeRole select Value,PolicyValue,WorkflowState,Id,OwnerId,Owner.InternalDisplayName,Property.Identifier,Property.DisplayName,Property.Id,AssignedResourceTypeWorkflowInstance.Identifier,AssignedResourceType.WorkflowInstanceId,AssignedResourceTypeRole.FullName,AssignedResourceTypeRole.DisplayName,StartDate,EndDate,AssignedResourceType.Id,AssignedResourceType.OwnerId,AssignedResourceType.OwnerType,AssignedResourceType.WorkflowState,AssignedResourceType.ProvisioningState,ProvisioningState,Owner.Op_MainRecord_FirstName,Owner.Op_MainRecord_LastName,Owner.Op_MainRecord_Email,Owner.Id,Owner.Op_MainRecord_Organization_DisplayName,Owner.Op_MainRecord_Site_DisplayName,Owner.Op_MainRecord_Login,Owner.Op_MainRecord_EmployeeId,Owner.Op_PresenceState_Id where (OwnerType=$owType AND ProvisioningState=1) order by AssignedResourceType.WorkflowInstanceId desc, Property.DisplayName desc, OwnerId desc, Id desc")
    $scalarPath  = [System.Web.HttpUtility]::UrlEncode("/Custom/ProvisioningPolicy/ReviewProvisioning/$srcET")
    $scalarResult = Request-UsercubeAPI -Verb GET -Url $scalarUrl -Squery "&squery=$scalarQuery&Path=$scalarPath"
    $scalars = if ($scalarResult.Result) { @($scalarResult.Result) } else { @() }

    # Fetch pending navigations
    $navUrl   = "$srvUrl/api/ProvisioningPolicy/AssignedResourceNavigation?api-version=1.0&getCurrentValues=true"
    $navQuery = [System.Web.HttpUtility]::UrlEncode("join Resource Resource join PolicyResource PolicyResource join Owner of type $srcET Owner join Property Property join AssignedResourceType AssignedResourceType join AssignedResourceType.WorkflowInstance AssignedResourceTypeWorkflowInstance join AssignedResourceType.Role AssignedResourceTypeRole select Resource.InternalDisplayName,PolicyResource.InternalDisplayName,IsDenied,WorkflowState,Id,OwnerId,Owner.InternalDisplayName,Property.Identifier,Property.DisplayName,Property.Id,AssignedResourceTypeWorkflowInstance.Identifier,AssignedResourceType.WorkflowInstanceId,AssignedResourceTypeRole.FullName,AssignedResourceTypeRole.DisplayName,StartDate,EndDate,AssignedResourceType.Id,AssignedResourceType.OwnerId,AssignedResourceType.OwnerType,AssignedResourceType.WorkflowState,AssignedResourceType.ProvisioningState,ProvisioningState,Owner.Op_MainRecord_FirstName,Owner.Op_MainRecord_LastName,Owner.Op_MainRecord_Email,Owner.Id,Owner.Op_MainRecord_Organization_DisplayName,Owner.Op_MainRecord_Site_DisplayName,Owner.Op_MainRecord_Login,Owner.Op_MainRecord_EmployeeId,Owner.Op_PresenceState_Id where (OwnerType=$owType AND ProvisioningState=1) order by AssignedResourceType.WorkflowInstanceId desc, Property.DisplayName desc, OwnerId desc, Id desc")
    $navPath  = [System.Web.HttpUtility]::UrlEncode("/Custom/ProvisioningPolicy/ReconciliateResources/$srcET")
    $navResult = Request-UsercubeAPI -Verb GET -Url $navUrl -Squery "&squery=$navQuery&Path=$navPath"
    $navs = if ($navResult.Result) { @($navResult.Result) } else { @() }

    # Group by OwnerId::ART.Id
    $groups = [ordered]@{}
    foreach ($s in $scalars) {
        $artId   = "$($s.AssignedResourceType.Id)"
        $ownerId = "$($s.OwnerId)"
        $key     = "${ownerId}::${artId}"
        if (-not $groups.Contains($key)) {
            $groups[$key] = [ordered]@{
                ownerId       = $ownerId
                artId         = $artId
                ownerName     = $s.Owner.InternalDisplayName
                roleName      = $s.AssignedResourceType.Role.DisplayName
                roleFullName  = $s.AssignedResourceType.Role.FullName
                workflowId    = $s.AssignedResourceType.WorkflowInstance.Identifier
                artProvState  = [int]$s.AssignedResourceType.ProvisioningState
                scalars       = [System.Collections.ArrayList]::new()
                navigations   = [System.Collections.ArrayList]::new()
            }
        }
        [void]$groups[$key].scalars.Add([ordered]@{
            id            = $s.Id
            propertyName  = $s.Property.DisplayName
            propertyIdent = $s.Property.Identifier
            propertyId    = $s.Property.Id
            value         = $s.Value
            policyValue   = $s.PolicyValue
            provState     = [int]$s.ProvisioningState
            startDate     = $s.StartDate
            endDate       = $s.EndDate
            workflowState = $s.WorkflowState
        })
    }

    foreach ($n in $navs) {
        $artId   = "$($n.AssignedResourceType.Id)"
        $ownerId = "$($n.OwnerId)"
        $key     = "${ownerId}::${artId}"
        if (-not $groups.Contains($key)) {
            $groups[$key] = [ordered]@{
                ownerId       = $ownerId
                artId         = $artId
                ownerName     = $n.Owner.InternalDisplayName
                roleName      = $n.AssignedResourceType.Role.DisplayName
                roleFullName  = $n.AssignedResourceType.Role.FullName
                workflowId    = $n.AssignedResourceType.WorkflowInstance.Identifier
                artProvState  = [int]$n.AssignedResourceType.ProvisioningState
                scalars       = [System.Collections.ArrayList]::new()
                navigations   = [System.Collections.ArrayList]::new()
            }
        }
        [void]$groups[$key].navigations.Add([ordered]@{
            id            = $n.Id
            propertyName  = $n.Property.DisplayName
            propertyIdent = $n.Property.Identifier
            propertyId    = $n.Property.Id
            resourceName  = $n.Resource.InternalDisplayName
            isDenied      = $n.IsDenied
            provState     = [int]$n.ProvisioningState
            startDate     = $n.StartDate
            endDate       = $n.EndDate
            workflowState = $n.WorkflowState
        })
    }

    return @($groups.Values)
}

# ── Fetch full user detail for one ART (all attrs, all states) ─────────────

function Get-UserARTDetail {
    param([string]$OwnerId, [string]$ARTId)

    $srvUrl = $script:UCServerURL
    $srcET  = $script:UCSourceET
    $owType = $script:UCOwnerType

    # Scalars for this ART
    $sUrl   = "$srvUrl/api/ProvisioningPolicy/AssignedResourceScalar/FromAssignedResourceType/$($ARTId)?api-version=1.0"
    $sQuery = [System.Web.HttpUtility]::UrlEncode("join Property arsp select arsp.Id, arsp.Identifier, arsp.DisplayName, Value, StartDate, EndDate, WorkflowState, ProvisioningState where (OwnerType=$owType AND OwnerId=$OwnerId) order by Id asc")
    $sPath  = [System.Web.HttpUtility]::UrlEncode("/Custom/Resources/$srcET/ViewOwnedResources")
    $sRoot  = [System.Web.HttpUtility]::UrlEncode($srcET)
    $sBind  = [System.Web.HttpUtility]::UrlEncode("Records.OwnerAssignedResourceTypes.AssignedResourceScalars")
    $sResult = Request-UsercubeAPI -Verb GET -Url $sUrl -Squery "&squery=$sQuery&Path=$sPath&QueryRootEntityType=$sRoot&QueryBinding=$sBind"
    $allScalars = if ($sResult.Result) { @($sResult.Result) } else { @() }

    # Navigations for this ART
    $nUrl   = "$srvUrl/api/ProvisioningPolicy/AssignedResourceNavigation/FromAssignedResourceType/$($ARTId)?api-version=1.0"
    $nQuery = [System.Web.HttpUtility]::UrlEncode("join Property arnp join Resource arnr select arnp.Id, arnp.Identifier, arnp.DisplayName, arnr.InternalDisplayName, StartDate, EndDate, IsDenied, WorkflowState, ProvisioningState, ConfidenceLevel, IsIndirect, IsInferred where (OwnerType=$owType AND OwnerId=$OwnerId) order by Id asc")
    $nPath  = [System.Web.HttpUtility]::UrlEncode("/Custom/Resources/$srcET/ViewOwnedResources")
    $nRoot  = [System.Web.HttpUtility]::UrlEncode($srcET)
    $nBind  = [System.Web.HttpUtility]::UrlEncode("Records.OwnerAssignedResourceTypes.AssignedResourceNavigations")
    $nResult = Request-UsercubeAPI -Verb GET -Url $nUrl -Squery "&squery=$nQuery&Path=$nPath&QueryRootEntityType=$nRoot&QueryBinding=$nBind"
    $allNavs = if ($nResult.Result) { @($nResult.Result) } else { @() }

    $scalarList = @($allScalars | ForEach-Object {
        [ordered]@{
            id            = $_.Id
            propertyName  = $_.Property.DisplayName
            propertyIdent = $_.Property.Identifier
            value         = $_.Value
            provState     = [int]$_.ProvisioningState
            workflowState = $_.WorkflowState
            startDate     = $_.StartDate
            endDate       = $_.EndDate
        }
    })

    $navList = @($allNavs | ForEach-Object {
        [ordered]@{
            id            = $_.Id
            propertyName  = $_.Property.DisplayName
            propertyIdent = $_.Property.Identifier
            resourceName  = $_.Resource.InternalDisplayName
            isDenied      = $_.IsDenied
            provState     = [int]$_.ProvisioningState
            workflowState = $_.WorkflowState
            startDate     = $_.StartDate
            endDate       = $_.EndDate
            confidenceLevel = $_.ConfidenceLevel
            isIndirect    = $_.IsIndirect
            isInferred    = $_.IsInferred
        }
    })

    return [ordered]@{
        scalars     = $scalarList
        navigations = $navList
    }
}

# ── Cancel functions ───────────────────────────────────────────────────────

function Invoke-CancelNavigation {
    param([string]$NavigationId)
    $url  = "$($script:UCServerURL)/api/ProvisioningPolicy/AssignedResourceNavigation/$($NavigationId)?api-version=1.0"
    $body = '{"ProvisioningState":5}'
    $null = Request-UsercubeAPI -Verb PUT -Url $url -Body $body
    return [ordered]@{ success = $true; message = "Navigation $NavigationId set to AwaitingApproval" }
}

function Invoke-CancelART {
    param([string]$ARTId)
    # This cancels ALL scalars AND navigations inside this ART (workaround for scalar cancel bug)
    $url  = "$($script:UCServerURL)/api/ProvisioningPolicy/AssignedResourceType/$($ARTId)?api-version=1.0"
    $body = '{"ProvisioningState":5}'
    $null = Request-UsercubeAPI -Verb PUT -Url $url -Body $body
    return [ordered]@{ success = $true; message = "ART $ARTId set to AwaitingApproval (all attributes)" }
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
    Write-Host "                          Records are never fully loaded into memory -- files are"
    Write-Host "                          streamed and filtered per query. Best for large datasets."
    Write-Host "                          Also enables the Pending tab for live order management."
    Write-Host "                          You will be prompted for Usercube credentials at startup."
    Write-Host "                          Opens browser automatically. Press Ctrl+C to stop."
    Write-Host ""
    Write-Host "    -SourceEntityType     Usercube source entity type identifier."
    Write-Host "                          Default: Directory_User"
    Write-Host ""
    Write-Host "    -Port [number]        Port for the HTTP server (default: 5005)."
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
    # Authenticate to Usercube API (interactive prompt)
    Add-Type -AssemblyName System.Web
    $script:UCSourceET = $SourceEntityType
    Initialize-UsercubeAuth

    # Serve mode: build a lightweight file index — no records held in memory.
    # Queries stream and filter files on demand, so memory stays low even for GB datasets.
    Write-Host "Building index (records will be streamed per query)..." -ForegroundColor Cyan
    $serveIndex = Build-ServeIndex -JsonFiles $allJsonFiles
    Write-Host "Index complete: $($serveIndex.OrderIndex.Count) orders." -ForegroundColor Green
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
