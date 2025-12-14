# Admin/Deployment Configuration

## For Regular Users - Ignore This!

This directory is for **automated deployment setup only**. You don't need anything here to use the literature search pipeline.

**Go to the main [README.md](../README.md)** for usage instructions.

---

## For Admins/Maintainers Only

This directory contains Prefect Cloud automation config:

- **Automated Schedule:** Every other Monday at 7:00 AM EST
- **Target:** Fetch 25 new papers per run
- **Retry Logic:** Up to 3 attempts if target not met

### Quick Commands

```bash
# Deploy/update (from this directory)
python deploy_scheduled.py

# Pause automation
prefect deployment pause Biweekly-Literature-Search/biweekly-literature-search

# Resume automation  
prefect deployment resume Biweekly-Literature-Search/biweekly-literature-search

# Check status
prefect deployment ls
```

See [PREFECT_CLOUD_SETUP.md](./PREFECT_CLOUD_SETUP.md) for full setup guide.
