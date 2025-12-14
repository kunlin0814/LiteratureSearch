#!/bin/bash
# Setup automated weekly literature search with Prefect

echo "Setting up weekly automated literature search..."
echo ""

# Create the deployment
python deploy_scheduled.py

echo ""
echo "=========================================="
echo "Deployment created successfully!"
echo "=========================================="
echo ""
echo "To activate the schedule, you need to keep these running:"
echo ""
echo "Terminal 1 - Prefect Server (leave running):"
echo "  prefect server start"
echo ""
echo "Terminal 2 - Prefect Agent (leave running):"
echo "  prefect agent start -q default"
echo ""
echo "=========================================="
echo "Alternative: Use Prefect Cloud (no need to run servers)"
echo "=========================================="
echo "  1. Sign up at https://app.prefect.cloud"
echo "  2. Run: prefect cloud login"
echo "  3. Deploy: python deploy_scheduled.py"
echo "  4. Done! Runs automatically in the cloud"
echo ""
