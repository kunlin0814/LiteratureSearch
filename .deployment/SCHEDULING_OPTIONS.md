# Alternative Option: macOS Cron Job for Weekly Literature Search
# This runs every Sunday at 6:00 AM

## Setup Instructions:

### 1. Open crontab editor:
```bash
crontab -e
```

### 2. Add this line (adjust paths to match your system):
```bash
# Run literature search every Sunday at 6:00 AM
0 6 * * 0 cd /Volumes/Research/GitHub/API_WF && /usr/local/bin/python3 Prefect_literatureSearch.py >> /tmp/literature_search.log 2>&1
```

### 3. Save and exit

### Cron Schedule Format:
```
* * * * *
│ │ │ │ │
│ │ │ │ └── Day of week (0-7, both 0 and 7 = Sunday)
│ │ │ └──── Month (1-12)
│ │ └────── Day of month (1-31)
│ └──────── Hour (0-23)
└────────── Minute (0-59)
```

### Examples:
- `0 6 * * 0` - Every Sunday at 6:00 AM
- `0 6 * * 1` - Every Monday at 6:00 AM  
- `0 6 1 * *` - First day of every month at 6:00 AM
- `0 */6 * * *` - Every 6 hours

### Verify it's scheduled:
```bash
crontab -l
```

### View logs:
```bash
tail -f /tmp/literature_search.log
```

---

## Comparison of Options

| Method | Pros | Cons |
|--------|------|------|
| **Prefect Deployments** | UI to monitor runs<br> Retry logic<br> Easy reschedule<br> Run history | Requires server running |
| **Prefect Cloud** |  No local servers<br> Always available<br> Web dashboard |  Cloud account needed |
| **Cron** | Simple<br> No dependencies<br> Native to macOS | No monitoring<br> Manual log checking |

---

## Recommended: Prefect Cloud (Best of Both Worlds)

1. Sign up at https://app.prefect.cloud (free tier available)
2. Run: `prefect cloud login`
3. Deploy: `python deploy_scheduled.py`
4. Done! Runs automatically with web monitoring

No servers to manage, full observability, automatic retries.
