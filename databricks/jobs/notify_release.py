"""Databricks job task: ask GitHub to deploy a newly promoted champion.

Runs after the train task. If train promoted a model, it published the version as the task value
"promoted_version"; this task then sends a repository_dispatch POST to GitHub, which starts the
"Release model" workflow (.github/workflows/release-model.yml). If nothing was promoted, it does nothing.
"""
import argparse
import json
import urllib.error
import urllib.request

from databricks.sdk.runtime import dbutils

EVENT_TYPE = "model-promoted"


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo", required=True, help="GitHub repository as owner/name")
    parser.add_argument("--model-name", required=True)
    parser.add_argument("--secret-scope", required=True)
    parser.add_argument("--train-task", default="train")
    return parser.parse_args()


def main():
    args = parse_args()
    version = dbutils.jobs.taskValues.get(taskKey=args.train_task, key="promoted_version", default=0, debugValue=0)
    if not version:
        print("train did not promote a new champion: nothing to release")
        return

    token = dbutils.secrets.get(args.secret_scope, "github-token")
    body = json.dumps({
        "event_type": EVENT_TYPE,
        "client_payload": {"version": int(version), "model_name": args.model_name},
    }).encode()
    request = urllib.request.Request(
        f"https://api.github.com/repos/{args.repo}/dispatches",
        data=body,
        method="POST",
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
            "Content-Type": "application/json",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            # GitHub answers 204 No Content when the event is accepted.
            print(f"GitHub accepted {EVENT_TYPE} for version {version} (HTTP {response.status})")
    except urllib.error.HTTPError as e:
        raise RuntimeError(
            f"GitHub rejected the dispatch (HTTP {e.code}): {e.read().decode(errors='replace')}. "
            "Check the github-token secret (fine-grained token, Contents: read and write on this repo)."
        ) from e
    except urllib.error.URLError as e:
        raise RuntimeError(
            f"Cannot reach api.github.com: {e.reason}. Add api.github.com to the allowed domains "
            "of the workspace's serverless network policy."
        ) from e


if __name__ == "__main__":
    main()
