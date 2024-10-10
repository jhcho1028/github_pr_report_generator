import requests
import pandas as pd
import time
import requests
from requests.exceptions import ConnectionError, Timeout, RequestException
from openpyxl import load_workbook
from openpyxl.utils import column_index_from_string
from datetime import datetime


# GitHub Personal Access Token
GITHUB_TOKEN = 'JH_TOKEN'

# GitHub API request headers
headers = {
    'Authorization': f'token {GITHUB_TOKEN}',
    'Accept': 'application/vnd.github.v3+json'
}

def get_rate_limit():
    """Fetch GitHub API rate limit."""
    rate_limit_url = 'https://api.github.com/rate_limit'
    rate_limit_response = requests.get(rate_limit_url, headers=headers)
    print(rate_limit_response.json())

def get_user_id(username):
    """Fetch the user ID from GitHub API based on username."""
    url = f'https://api.github.com/users/{username}'
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        user_info = response.json()
        return user_info['id']  # Get the unique user ID
    else:
        print(f"Error fetching user info for {username}")
        return None
    
def get_repositories_from_excel(repo_excel_path, repo_sheet_name, column_letter):
    """Load repositories from the Excel file's specified column."""
    print(f"Loading repositories from Excel file '{repo_excel_path}', sheet '{repo_sheet_name}'...")
    wb = load_workbook(repo_excel_path)
    ws = wb[repo_sheet_name]

    repos = []
    column_index = column_index_from_string(column_letter) - 1

    for row in ws.iter_rows(min_row=2, values_only=True):  # Skip header row
        repo_name = row[column_index]
        if repo_name:
            repos.append(repo_name)
        else:
            print("Skipping empty repository name")

    print(f"Found {len(repos)} repositories.")
    return repos

def get_contributors_from_excel(repo_excel_path, contributor_sheet_name, column_letter):
    """Load contributors from the Excel file's specified column."""
    print(f"Loading contributors from Excel file '{repo_excel_path}', sheet '{contributor_sheet_name}'...")
    wb = load_workbook(repo_excel_path)
    ws = wb[contributor_sheet_name]

    contributors = []
    column_index = column_index_from_string(column_letter) - 1

    for row in ws.iter_rows(min_row=1, values_only=True):  # Skip header row
        contributor_name = row[column_index]
        if contributor_name:
            contributors.append(contributor_name)
        else:
            print("Skipping empty contributor name")

    print(f"Found {len(contributors)} contributors.")
    return contributors

def get_prs_for_repository(repo_name):
    """Fetch PRs for a given repository."""
    print(f"\nFetching PR list for repository '{repo_name}'...")
    all_prs = []
    page = 1

    while True:
        url = f'https://api.github.com/repos/AdvancedTechnologyInc/{repo_name}/pulls?state=all&per_page=100&page={page}'
        try:
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()  # Raise an exception for bad responses
            prs = response.json()
            if not prs:
                break
            print(f"Found {len(prs)} PRs on page {page}.")
            all_prs.extend(prs)
            page += 1
            time.sleep(1)  # 1초 대기 후 다음 요청
        except RequestException as e:
            print(f"Error fetching PR list for repository '{repo_name}': {e}")
            break
        
    return all_prs

def get_pr_details(repo_name, pr_number):
    """Fetch the details for a specific PR."""
    pr_detail_url = f"https://api.github.com/repos/AdvancedTechnologyInc/{repo_name}/pulls/{pr_number}"
    pr_detail_response = requests.get(pr_detail_url, headers=headers)
    if pr_detail_response.status_code == 200:
        pr_details = pr_detail_response.json()
        additions = pr_details.get('additions', 0)
        deletions = pr_details.get('deletions', 0)
        total_changes = additions + deletions
        return total_changes
    else:
        print(f"Failed to fetch details for PR #{pr_number}")
        return 'N/A'

def calculate_merge_time(created_at, closed_at):
    """Calculate the time taken to merge a PR."""
    if created_at and closed_at:
        created_time = datetime.strptime(created_at, "%Y-%m-%dT%H:%M:%SZ")
        closed_time = datetime.strptime(closed_at, "%Y-%m-%dT%H:%M:%SZ")
        return (closed_time - created_time).days
    return 'N/A'

def extract_data_from_prs(prs, repo_name, user_id):
    """Extract relevant PR data and include merge/cancel status."""
    data = []
    for pr in prs:
        # Check if the PR is created by the contributor (user_id)
        if pr['user']['id'] == user_id:
            pr_number = pr['number']
            pr_title = pr['title']
            pr_link = pr['html_url']
            related_issue = pr.get('body', '').split('Related issue: ')[-1] if 'Related issue: ' in pr.get('body', '') else ''
            created_at = pr['created_at']  # PR open time
            closed_at = pr['closed_at']  # PR close time (if merged or closed)
            merged_at = pr['merged_at']  # PR merge time (None if not merged)

            # Determine merge/cancel status
            if merged_at:
                merge_status = 'Merged'
            elif closed_at and not merged_at:
                merge_status = 'Cancelled'
            else:
                merge_status = 'Open'

            time_to_merge = calculate_merge_time(created_at, closed_at)
            total_changes = get_pr_details(repo_name, pr_number)

            # Append the data including time info and merge status
            data.append([pr_number, repo_name, pr_title, pr_link, related_issue, created_at, closed_at, time_to_merge, total_changes, merge_status])
        else:
            print(f"PR #{pr['number']} is not created by the target user.")
    
    return data


def save_to_excel(data, output_path):
    """Save PR data to an Excel file with hyperlinks and No. column."""
    print("\nSaving data to Excel...")
    
    # Add the 'No.' column by adding index numbers to the data
    numbered_data = [[i + 1] + row for i, row in enumerate(data)]
    
    # Create DataFrame with added 'No.' column
    df = pd.DataFrame(numbered_data, columns=['No.', 'Number', 'Repository', 'PR Title', 'PR Link', 'Related Issue', 'PR Open Time', 'PR Close Time', 'Merge Time (days)', 'Lines Changed', 'PR Merged Status'])

    # Write DataFrame to Excel
    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='PR List')
        wb = writer.book
        ws = wb['PR List']

        # Add hyperlinks to the 'PR Link' column
        for row in range(2, len(df) + 2):
            cell = ws.cell(row=row, column=5)  # PR Link is in the 5th column now
            cell.hyperlink = cell.value  # Set the hyperlink
            cell.style = 'Hyperlink'  # Apply hyperlink style

    print(f"PR list has been saved to '{output_path}' with hyperlinks.")

def main():
    # Excel settings
    repo_excel_path = '240930.xlsx'
    repo_sheet_name = 'RND'
    repo_column_letter = 'H'  # Repo name in H column
    contributor_sheet_name = 'Contributors'
    contributor_column_letter = 'A'  # Contributor name in A column

    # Step 1: Check rate limit
    get_rate_limit()

    # Step 2: Fetch repository names from Excel sheet
    repos = get_repositories_from_excel(repo_excel_path, repo_sheet_name, repo_column_letter)

    # Step 3: Fetch contributors from Excel sheet
    contributors = get_contributors_from_excel(repo_excel_path, contributor_sheet_name, contributor_column_letter)

    # 사용자 ID 캐시용 딕셔너리
    user_ids = {}

    # Step 4: Fetch PRs and extract data for each contributor
    for contributor in contributors:
        print(f"Processing PRs for contributor '{contributor}' across all repositories...")
        
        # 기여자별 PR 데이터를 누적할 리스트
        contributor_data = []

        # 기여자 ID 캐시에서 확인
        if contributor not in user_ids:
            user_id = get_user_id(contributor)
            if user_id:
                user_ids[contributor] = user_id
            else:
                print(f"Skipping contributor '{contributor}' due to missing user ID.")
                continue  # 사용자 ID를 못 얻으면 다음으로 넘어감
        else:
            user_id = user_ids[contributor]  # 캐시된 사용자 ID 사용

        # 모든 저장소에 대해 PR 데이터 처리
        for repo_name in repos:
            prs = get_prs_for_repository(repo_name)
            repo_contributor_data = extract_data_from_prs(prs, repo_name, user_id)
            contributor_data.extend(repo_contributor_data)  # 각 저장소 데이터를 누적

        # Step 5: 기여자의 모든 데이터를 Excel에 저장
        if contributor_data:
            output_path = f'{contributor}_pr_list.xlsx'
            save_to_excel(contributor_data, output_path)
        else:
            print(f"No PR data found for contributor '{contributor}' in any repository.")

if __name__ == '__main__':
    main()
