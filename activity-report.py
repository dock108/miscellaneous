import os
import json
import time
import requests
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict, Any

# ----------------------- CONFIG -----------------------
# File paths and API base URL
CACHE_FILE = "activity_cache.json"
ERROR_LOG_FILE = "error_log.txt"
REPO_AUDIT_FILE = "repo_audit_log.csv"
OUTPUT_EXCEL_FILE = "github_user_activity_report.xlsx"
GITHUB_API = "https://api.github.com"

# ----------------------- UTILS -----------------------
def log_error(message: str):
    """Append error messages to an error log with timestamp."""
    with open(ERROR_LOG_FILE, "a") as f:
        f.write(f"{datetime.utcnow().isoformat()} - {message}\n")

def rate_limit_guard(response):
    """Check and handle GitHub API rate limits by sleeping until reset."""
    if response.status_code == 403 and 'X-RateLimit-Remaining' in response.headers:
        remaining = int(response.headers['X-RateLimit-Remaining'])
        if remaining == 0:
            reset = int(response.headers['X-RateLimit-Reset'])
            sleep_time = reset - int(time.time()) + 1
            print(f"Rate limit hit. Sleeping for {sleep_time} seconds...")
            time.sleep(sleep_time)

# ----------------------- GITHUB HELPERS -----------------------
def github_request(session, url, headers):
    """Perform a GET request to the GitHub API and handle rate limits and errors."""
    try:
        response = session.get(url, headers=headers)
        rate_limit_guard(response)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        log_error(f"Failed request to {url}: {e}")
        return None

# ----------------------- MAIN FUNCTION -----------------------
def check_user_activity(token: str, orgs: List[str], user_ids: List[str]):
    """
    Main driver to evaluate user activity across GitHub organizations.

    Parameters:
    - token: GitHub access token
    - orgs: list of organization names
    - user_ids: list of GitHub usernames to track

    Returns:
    - Writes Excel report with user activity and repo audit logs
    """
    headers = {"Authorization": f"token {token}"}
    session = requests.Session()

    # Load or initialize activity cache
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE, "r") as f:
            cache = json.load(f)
    else:
        cache = {}

    # Initialize user summary structure
    user_summary = {
        uid: {
            "default_commit": False,
            "any_commit": False,
            "any_activity": False,
            "default_commit_source": None,
            "any_commit_source": None,
            "any_activity_source": None,
            "last_push_date": None
        } for uid in user_ids
    }
    repo_audit = []

    for org in orgs:
        # Paginate through repositories in each org
        repos_url = f"{GITHUB_API}/orgs/{org}/repos?per_page=100&type=all"
        page = 1

        while True:
            url = f"{repos_url}&page={page}"
            repos = github_request(session, url, headers)
            if not repos:
                break
            if len(repos) == 0:
                break

            for repo in repos:
                repo_name = repo.get("name")
                full_name = repo.get("full_name")
                default_branch = repo.get("default_branch")

                repo_key = f"{org}/{repo_name}"
                if repo_key not in cache:
                    cache[repo_key] = {}

                print(f"Scanning {repo_key}...")
                repo_audit.append({"repo": repo_key, "default_branch": default_branch})

                # Check commits to default branch in last 60 days
                since_60 = (datetime.utcnow() - timedelta(days=60)).isoformat() + "Z"
                default_commits_url = f"{GITHUB_API}/repos/{org}/{repo_name}/commits?sha={default_branch}&since={since_60}&per_page=100"
                default_commits = github_request(session, default_commits_url, headers)
                if default_commits:
                    for commit in default_commits:
                        author = commit.get("author", {}).get("login")
                        if author in user_ids and not user_summary[author]["default_commit"]:
                            user_summary[author]["default_commit"] = True
                            user_summary[author]["default_commit_source"] = repo_key

                # Check commits to any branch in last 30 days
                since_30 = (datetime.utcnow() - timedelta(days=30)).isoformat() + "Z"
                branches_url = f"{GITHUB_API}/repos/{org}/{repo_name}/branches?per_page=100"
                branches = github_request(session, branches_url, headers)
                if not branches:
                    continue
                for branch in branches:
                    branch_name = branch.get("name")
                    branch_commits_url = f"{GITHUB_API}/repos/{org}/{repo_name}/commits?sha={branch_name}&since={since_30}&per_page=100"
                    branch_commits = github_request(session, branch_commits_url, headers)
                    if branch_commits:
                        for commit in branch_commits:
                            author = commit.get("author", {}).get("login")
                            if author in user_ids and not user_summary[author]["any_commit"]:
                                user_summary[author]["any_commit"] = True
                                user_summary[author]["any_commit_source"] = f"{repo_key}@{branch_name}"

                # Check any public repo events
                events_url = f"{GITHUB_API}/repos/{org}/{repo_name}/events"
                events = github_request(session, events_url, headers)
                if events:
                    for event in events:
                        actor = event.get("actor", {}).get("login")
                        if actor in user_ids and not user_summary[actor]["any_activity"]:
                            user_summary[actor]["any_activity"] = True
                            user_summary[actor]["any_activity_source"] = f"{repo_key}:{event.get('type')}"

                # Look for last commit up to 90 days ago if no recent commits
                since_90 = (datetime.utcnow() - timedelta(days=90)).isoformat() + "Z"
                for branch in branches:
                    branch_name = branch.get("name")
                    commits_url = f"{GITHUB_API}/repos/{org}/{repo_name}/commits?sha={branch_name}&since={since_90}&per_page=100"
                    commits = github_request(session, commits_url, headers)
                    if commits:
                        for commit in commits:
                            author = commit.get("author", {}).get("login")
                            date = commit.get("commit", {}).get("author", {}).get("date")
                            if author in user_ids and not user_summary[author]["any_commit"]:
                                prev_date = user_summary[author]["last_push_date"]
                                if not prev_date or date > prev_date:
                                    user_summary[author]["last_push_date"] = date

                # Save progress
                with open(CACHE_FILE, "w") as f:
                    json.dump(cache, f)

                # Stop processing users who have all 3 conditions met
                user_ids = [uid for uid in user_ids if not all([
                    user_summary[uid]["default_commit"],
                    user_summary[uid]["any_commit"],
                    user_summary[uid]["any_activity"]
                ])]
                if not user_ids:
                    break

            if not user_ids:
                break
            page += 1

    # Final summary table
    summary_df = pd.DataFrame([
        {
            "User ID": uid,
            "Commit to Default Branch (Last 60 Days)": user_summary[uid]["default_commit"],
            "Commit to Any Branch (Last 30 Days)": user_summary[uid]["any_commit"],
            "Any Activity (Last 30 Days)": user_summary[uid]["any_activity"],
            "Last Push Seen (if no commit in last 30d)": user_summary[uid]["last_push_date"],
            "Source (default commit)": user_summary[uid]["default_commit_source"],
            "Source (any commit)": user_summary[uid]["any_commit_source"],
            "Source (any activity)": user_summary[uid]["any_activity_source"]
        }
        for uid in user_summary
    ])

    # Repo audit log
    audit_df = pd.DataFrame(repo_audit)

    # Save both tabs to Excel
    with pd.ExcelWriter(OUTPUT_EXCEL_FILE, engine='xlsxwriter') as writer:
        summary_df.to_excel(writer, index=False, sheet_name='Summary')
        audit_df.to_excel(writer, index=False, sheet_name='RepoAudit')

    print(f"✅ Finished! Output saved to {OUTPUT_EXCEL_FILE}")

# ----------------------- ENTRY POINT -----------------------
if __name__ == "__main__":
    YOUR_TOKEN = os.getenv("GH_TOKEN") or "ghp_yourtokenhere"
    ORGS = ["org1", "org2"]  # Replace with your GitHub orgs
    USER_IDS = ["username1", "username2"]  # Replace with GitHub usernames

    check_user_activity(YOUR_TOKEN, ORGS, USER_IDS)
