#!/usr/bin/env python3
"""
Prefect Cloud deployment with biweekly schedule.
Runs every other Monday at 7:00 AM to fetch 25 new papers.

Uses Prefect 3.x deployment API.
"""

if __name__ == "__main__":
    import sys
    import os
    
    # Add .deployment directory to path so we can import biweekly_flow
    script_dir = os.path.dirname(os.path.abspath(__file__))
    parent_dir = os.path.dirname(script_dir)  # Root directory
    sys.path.insert(0, script_dir)  # For biweekly_flow
    sys.path.insert(0, parent_dir)  # For literature_flow, modules, etc.
    
    # Import the biweekly wrapper flow
    from biweekly_flow import biweekly_literature_search_flow
    from prefect.client.schemas.schedules import RRuleSchedule
    
    # Define the GitHub source
    # This tells Prefect to clone this repo before running
    # IMPORTANT: You must push your latest code to GitHub for this to work!
    flow_from_source = biweekly_literature_search_flow.from_source(
        source="https://github.com/kunlin0814/LiteratureSearch.git",
        entrypoint=".deployment/biweekly_flow.py:biweekly_literature_search_flow"
    )
    
    # Deploy using Prefect Managed pool
    deployment_id = flow_from_source.deploy(
        name="biweekly-literature-search",
        work_pool_name="literature-managed-pool",
        schedule=RRuleSchedule(
            # Recurrence Rule: Every 2 weeks on Monday at 7:00 AM
            # DTSTART sets the first run and anchor time
            rrule="DTSTART:20251215T070000\nFREQ=WEEKLY;INTERVAL=2;BYDAY=MO",
            timezone="America/New_York"
        ),
        tags=["literature", "biweekly", "automated"],
        parameters={
            "max_results": 25,  # Target 25 new papers
            "max_retries": 3    # Retry up to 3 times
        },
        description="Automated biweekly literature search fetching 25 new papers"
    )
    
    print("Deployment created: 'biweekly-literature-search'")
    print("Source: https://github.com/kunlin0814/LiteratureSearch.git")
    print("Pool: literature-managed-pool (Serverless)")
    print("Schedule: Every Monday at 7:00 AM EST")
    print("Target: 25 new papers per run (with up to 3 retries)")
    
    print("\n IMPORTANT Step for Cloud Execution:")
    print("Since we are running serverless, Prefect needs to download your code from GitHub.")
    print("You MUST push your latest changes:")
    print("git add .")
    print("git commit -m 'Add biweekly automation'")
    print("git push")
    
    print("\n" + "="*60)
    print("PREFECT CLOUD SETUP (Recommended - Free Tier)")
    print("="*60)
    print("\n1. Create free account:")
    print("https://app.prefect.cloud")
    print("\n2. Login from terminal:")
    print("prefect cloud login")
    print("\n3. Run this script:")
    print("python deploy_scheduled.py")
    print("\n4. Monitor at https://app.prefect.cloud")
    print("\n" + "="*60)
