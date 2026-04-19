# NIM LogViewer

Interactive viewer for **Netwrix Identity Manager** provisioning order logs.

## What it does

Scans a folder of provisioning order JSON files and lets you explore, filter, and export them through a browser UI.

Two modes:

| Mode | Command | Best for |
|------|---------|----------|
| **File** | `.\LogViewer.ps1 -BasePath "..."` | Up to ~80k orders — generates a self-contained `LogViewer.html` you can share |
| **Serve** | `.\LogViewer.ps1 -BasePath "..." -Serve` | Large datasets (GBs) — starts a local HTTP server, data stays in memory |

## Usage

```powershell
# File mode — generates LogViewer.html next to the script
.\LogViewer.ps1 -BasePath "C:\path\to\ProvisioningOrders"

# File mode with date range
.\LogViewer.ps1 -BasePath "C:\path\to\ProvisioningOrders" -StartDate 2026-01-01 -EndDate 2026-03-31

# Serve mode — opens browser automatically
.\LogViewer.ps1 -BasePath "C:\path\to\ProvisioningOrders" -Serve -Port 8081

# Serve mode with request logging
.\LogViewer.ps1 -BasePath "C:\path\to\ProvisioningOrders" -Serve -Verbose

# Help
.\LogViewer.ps1 -Help
```

## Features

- Filter by date range, change type, source/target entity, owner search
- Filter by number of changes (min/max) and specific attribute name/value
- Filter by role direction (added/removed) and count
- Timeline chart with drag-to-select date range
- Expandable rows showing full change diffs and roles
- Owner details modal
- Lifecycle tab — search an owner's full provisioning history
- CSV export
- Linked entity dropdowns (selecting a source scopes the target list and vice versa)

## Requirements

- PowerShell 5.1+
- The provisioning orders folder structure expected: `<BasePath>/<ResourceType_Id>/<date>_*.json`
