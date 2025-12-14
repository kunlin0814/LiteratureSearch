# Prefect Cloud Setup - Biweekly Literature Search

## Quick Start (5 minutes)

### 1. Create Prefect Cloud Account (Free)
Visit https://app.prefect.cloud and sign up

### 2. Login from Terminal
```bash
cd /Volumes/Research/GitHub/API_WF
prefect cloud login
```
Follow the prompts to authenticate

### 3. Deploy Your Pipeline
```bash
python deploy_scheduled.py
```

### 4. Done! 
Your pipeline is now deployed and will run automatically:
- **Every other Monday at 7:00 AM EST**
- **Fetches 25 new papers** (with up to 3 retry attempts)
- **Runs even when your computer is off** (runs in Prefect Cloud)

---

## How It Works

### Biweekly Schedule
- Runs **every Monday** at 7:00 AM
- Flow checks if it's a **biweekly Monday** (odd ISO week numbers)
- Skips on even weeks automatically

### Smart Retry Logic
1. Attempts to fetch 25 NEW papers (not already in Notion)
2. If fewer than 25 are found, retries up to 3 times
3. Each retry increases the search window
4. Exits gracefully if target cannot be reached

### What Gets Saved
- Only truly NEW papers (checked against Notion database)
- All papers are enriched with AI metadata
- Duplicates are automatically skipped

---

## Monitoring

### View Runs
https://app.prefect.cloud → Your Workspace → Flows

### Check Logs
Click any flow run to see detailed logs, including:
- How many papers were fetched
- How many retries were needed
- Any errors or issues

### Notifications
Set up Slack/Email alerts in Prefect Cloud settings

---

## Configuration

### Change Schedule
Edit `deploy_scheduled.py`:
```python
cron="0 7 * * 1",  # Every Monday 7 AM
timezone="America/New_York"
```

Cron format: `minute hour day month weekday`

Examples:
- `0 7 * * 1` - Every Monday 7 AM
- `0 9 * * 3` - Every Wednesday 9 AM  
- `30 18 * * 5` - Every Friday 6:30 PM

### Change Target Papers
Edit `deploy_scheduled.py`:
```python
parameters={
    "max_results": 50,  # Change from 25 to 50
    "max_retries": 5    # Change from 3 to 5
}
```

### Change Search Window
Edit `biweekly_flow.py`:
```python
rel_date_days: Optional[int] = 14,  # Change from 14 to 30
```

---

## Manual Testing

### Test Locally (Without Schedule)
```bash
python biweekly_flow.py --force --max-results 10
```
The `--force` flag bypasses the biweekly check

### Test Single Run
```bash
python biweekly_flow.py --max-results 25 --max-retries 3
```

---

## Troubleshooting

### "Not a biweekly Monday" in logs
 This is normal! The flow checks ISO week numbers and skips even weeks

### "Failed after 3 retries"
- Check if there are actually new papers in PubMed
- Consider increasing `max_retries` or `rel_date_days`

### Pipeline not running automatically
1. Check deployment: `prefect deployment ls`
2. Verify schedule in Prefect Cloud UI
3. Check work pool status

---

## Cost

**Prefect Cloud Free Tier:**
-  20,000 flow runs/month
-  1GB storage
-  3 users
-  Unlimited deployments

Your biweekly schedule = **~2 runs/month** = Well within free tier!

---

## Next Steps

1. **Monitor First Run**: Check Prefect Cloud after the first Monday
2. **Adjust Parameters**: Tweak `max_results`, `max_retries` based on results
3. **Set Up Alerts**: Configure Slack/Email notifications in Prefect Cloud

Questions? Check logs at https://app.prefect.cloud
