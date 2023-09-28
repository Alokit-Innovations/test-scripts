import requests
import json
import logging
import time
import psycopg2
import os
import datetime
from dotenv import load_dotenv

# load environment variables
load_dotenv()

# set up logging
logging.basicConfig(filename='gcscruncsql.log', level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration for the script
oauth_consumer_key = os.getenv('test_oauth_consumer_key')
oauth_consumer_secret = os.getenv('test_oauth_consumer_secret')
db_host = os.getenv('test_db_host')
db_port = os.getenv('test_db_port')
db_name = os.getenv('test_db_name')
db_user = os.getenv('test_db_user')
db_password = os.getenv('test_db_password')
workspace = 'alokit-innovations-test'
repo_name = 'on-prem-bitbucket-test-repo'
webhook_url = 'https://968d-171-76-83-56.ngrok-free.app/api/bitbucket/callbacks/webhook'

# Get the OAuth token
def get_oauth_token(client_id, client_secret):
    url = "https://bitbucket.org/site/oauth2/access_token"
    response = requests.post(url, auth=(client_id, client_secret), data={'grant_type': 'client_credentials'})
    response.raise_for_status()
    return response.json()

def create_repo(workspace, repo_name, token):
    url = f"https://api.bitbucket.org/2.0/repositories/{workspace}/{repo_name}"
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.post(url, headers=headers, json={"scm": "git"})
    response.raise_for_status()
    return response.json()

def create_branch(workspace, repo_name, token, source_branch, destination_branch):
    url = f"https://api.bitbucket.org/2.0/repositories/{workspace}/{repo_name}/refs/branches"
    headers = {"Authorization": f"Bearer {token}"}
    data = {"name": source_branch, "target": {"hash": destination_branch}}
    response = requests.post(url, headers=headers, json=data)
    response.raise_for_status()

def add_and_commit_change(workspace, repo_name, token, branch, filename, content):
    url = f"https://api.bitbucket.org/2.0/repositories/{workspace}/{repo_name}/src"
    headers = {"Authorization": f"Bearer {token}"}
    data = {"message": "Add/Update file", "branch": branch}
    files = {filename: (filename, content)}
    response = requests.post(url, headers=headers, data=data, files=files)
    response.raise_for_status()
    return response.json()

def raise_pr(workspace, repo_name, token):
    source_branch = "feature/dummy"
    destination_branch = "main"
    
    create_branch(workspace, repo_name, token, source_branch, destination_branch)
    
    filename = "dummy_file.txt"
    content = 'print("This is a modified dummy file")'
    add_and_commit_change(workspace, repo_name, token, source_branch, filename, content)
    
    url = f"https://api.bitbucket.org/2.0/repositories/{workspace}/{repo_name}/pullrequests"
    headers = {"Authorization": f"Bearer {token}"}
    data = {
        "title": "Dummy PR",
        "source": {"branch": {"name": source_branch}},
        "destination": {"branch": {"name": destination_branch}},
        "close_source_branch": True,
        "reason": "Merging modified dummy feature",
    }
    response = requests.post(url, headers=headers, json=data)
    response.raise_for_status()
    return response.json()

def store_repo_data(name, workspace, auth_info, provider,metadata, git_url):
    conn = psycopg2.connect(f"dbname={db_name} user={db_user} host={db_host} password={db_password} port={db_port}")
    cur = conn.cursor()
    query = """
        INSERT INTO repos (repo_name, repo_owner, repo_provider, auth_info, metadata, git_url) 
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (repo_name, repo_owner, repo_provider) DO UPDATE SET 
        auth_info = EXCLUDED.auth_info, 
        metadata = EXCLUDED.metadata,
        git_url = EXCLUDED.git_url
    """
    params = (name, workspace, provider, auth_info, metadata, git_url)
    cur.execute(query, params)
    conn.commit()
    cur.close()
    conn.close()

def simulate_webhook_event(webhook_url, pr_info, repo_data):
    payload = {
        "pullrequest": pr_info,
        "repository": repo_data
    }
    response = requests.post(webhook_url, json=payload)
    response.raise_for_status()

def check_db_for_hunk_info(pr_number, repo_name, repo_owner, provider):
    time.sleep(180)
    conn = psycopg2.connect(f"dbname={db_name} user={db_user} host={db_host} password={db_password} port={db_port}")
    cur = conn.cursor()
    cur.execute("SELECT hunk_info FROM hunks WHERE review_id=%s and repo_name=%s and repo_owner=%s and repo_provider=%s", (pr_number, repo_name, repo_owner, provider))
    row = cur.fetchone()
    cur.close()
    conn.close()
    return bool(row)

def delete_repo(workspace, repo_name, token):
    url = f"https://api.bitbucket.org/2.0/repositories/{workspace}/{repo_name}"
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.delete(url, headers=headers)
    response.raise_for_status()

def main():
    auth_info = get_oauth_token(oauth_consumer_key, oauth_consumer_secret)
    expires_at = datetime.datetime.utcnow() + datetime.timedelta(seconds=auth_info["expires_in"])
    expires_at_formatted = expires_at.strftime("%Y-%m-%dT%H:%M:%SZ") ## Change the `expires_in` field to `expires_at` format

    auth_info = { "access_token": auth_info["access_token"],
		"expires_at": expires_at_formatted,
		"refresh_token": auth_info["refresh_token"],
        "worspace_slug": ['alokit_innovations_test']}
    
    repo_info = create_repo(workspace, repo_name, auth_info["access_token"])
    
    metadata = json.dumps({ "provider_repo_id": repo_info["uuid"]}),
    git_url = [repo_info["links"]["clone"][1]["href"]]
    
    store_repo_data(repo_name, workspace, json.dumps(auth_info), metadata, git_url)
    pr_info = raise_pr(workspace, repo_name, auth_info["access_token"])
    simulate_webhook_event(webhook_url, pr_info, repo_info)

    if check_db_for_hunk_info(pr_info['id']):
        print("Hunk info is stored in the database.")

    delete_repo(workspace, repo_name, auth_info["access_token"])

if __name__ == "__main__":
    main()