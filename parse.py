

import os
import re
import pandas as pd
import time
from threading import Thread

from github import Github
from github.GithubException import RateLimitExceededException, GithubException

# using an access token with retry on rate limit
g = Github(os.environ['GITHUB_ACCESS_TOKEN'], retry=3, timeout=30)


def extract_repo(url):
    reu = re.compile(r'^https://github.com/([\w-]+/[-\w\.]+)$')
    m = reu.match(url)
    if m:
        return m.group(1)
    else:
        return ''


def get_cran_publication_date(package_url):
    """Extract publication date for CRAN packages from the URL."""
    # For CRAN packages, we could potentially scrape the publication date
    # but for now, return a placeholder date to indicate it's a CRAN package
    if 'cran.r-project.org' in package_url:
        return '1999-01-01'  # Use a very old date to indicate CRAN package
    return ''


def get_last_commit(repo):
    """Get last commit date for a GitHub repository with rate limiting and error handling."""
    if not repo:
        return ''
    
    max_retries = 3
    base_delay = 1
    
    for attempt in range(max_retries):
        try:
            r = g.get_repo(repo)
            cs = r.get_commits()
            return cs[0].commit.author.date.strftime('%Y-%m-%d')
        except RateLimitExceededException:
            # Wait for rate limit to reset
            rate_limit = g.get_rate_limit()
            reset_time = rate_limit.core.reset
            sleep_time = (reset_time - time.time()) + 10  # Add 10 second buffer
            print(f'Rate limit exceeded for {repo}. Waiting {sleep_time:.0f} seconds...')
            time.sleep(max(sleep_time, 60))  # Wait at least 60 seconds
            continue
        except GithubException as e:
            if e.status == 404:
                print(f'Repository not found: {repo}')
                return ''
            elif e.status == 403:
                # Rate limit or access denied - implement exponential backoff
                delay = base_delay * (2 ** attempt)
                print(f'Access denied for {repo} (attempt {attempt + 1}/{max_retries}). Waiting {delay} seconds...')
                time.sleep(delay)
                continue
            else:
                print(f'GitHub API error for {repo}: {e.status} - {e.data.get("message", "Unknown error")}')
                return ''
        except Exception as e:
            print(f'Unexpected error for {repo}: {str(e)}')
            return ''
    
    print(f'Failed to get last commit for {repo} after {max_retries} attempts')
    return ''


class Project(Thread):

    def __init__(self, match, section):
        super().__init__()
        self._match = match
        self.regs = None
        self._section = section

    def run(self):
        m = self._match
        is_github = 'github.com' in m.group(2)
        is_cran = 'cran.r-project.org' in m.group(2)
        repo = extract_repo(m.group(2))
        print(f'Processing: {m.group(1)} - {repo if repo else m.group(2)}')
        
        # Add a small delay between requests to be respectful to the API
        time.sleep(0.1)
        
        # Get last commit date or publication date
        if is_cran and not repo:
            # For CRAN packages without GitHub repos
            last_commit = get_cran_publication_date(m.group(2))
        else:
            last_commit = get_last_commit(repo)
        self.regs = dict(
            project=m.group(1),
            section=self._section,
            last_commit=last_commit,
            url=m.group(2),
            description=m.group(3),
            github=is_github,
            cran=is_cran,
            repo=repo
        )


projects = []

# Read all projects from README.md first

with open('README.md', 'r', encoding='utf8') as f:
    ret = re.compile(r'^(#+) (.*)$')
    rex = re.compile(r'^\s*- \[(.*)\]\((.*)\) - (.*)$')
    m_titles = []
    last_head_level = 0
    for line in f:
        m = rex.match(line)
        if m:
            p = Project(m, ' > '.join(m_titles[1:]))
            p.start()
            projects.append(p)
        else:
            m = ret.match(line)
            if m:
                hrs = m.group(1)
                if len(hrs) > last_head_level:
                    m_titles.append(m.group(2))
                else:
                    for n in range(last_head_level - len(hrs) + 1):
                        m_titles.pop()
                    m_titles.append(m.group(2))
                last_head_level = len(hrs)

print(f'Found {len(projects)} projects to process...')

# Start processing projects with some delay between batches to respect rate limits
batch_size = 10
for i in range(0, len(projects), batch_size):
    batch = projects[i:i+batch_size]
    print(f'Starting batch {i//batch_size + 1}/{(len(projects) + batch_size - 1)//batch_size} ({len(batch)} projects)...')
    
    # Start batch of threads
    for p in batch:
        p.start()
    
    # Wait for batch to complete before starting next batch
    batch_complete = False
    while not batch_complete:
        time.sleep(1)
        batch_complete = all(not p.is_alive() for p in batch)
    
    # Small delay between batches
    if i + batch_size < len(projects):
        print('Batch complete. Waiting before next batch...')
        time.sleep(2)

projects = [p.regs for p in projects]
df = pd.DataFrame(projects)
df.to_csv('site/projects.csv', index=False)
# df.to_markdown('projects.md', index=False)
