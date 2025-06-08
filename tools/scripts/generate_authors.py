import shutil
import re
from datetime import datetime
from collections import defaultdict
import subprocess  # nosec B404


def get_git_command():
    git_path = shutil.which("git")
    if not git_path:
        raise EnvironmentError("Git executable not found in system PATH.")
    return git_path


def get_contributors():
    git = get_git_command()
    try:
        output = subprocess.check_output(
            [git, "shortlog", "-sne", "--all"], encoding="utf-8"
        )  # nosec B603
    except subprocess.CalledProcessError as e:
        raise RuntimeError("Failed to get git shortlog output.") from e

    lines = output.strip().split("\n")
    contributors = defaultdict(int)

    for line in lines:
        if not line.strip():
            continue

        parts = line.strip().split("\t")
        if len(parts) != 2:
            continue

        count = int(parts[0].strip())
        name_email = parts[1].strip()

        name_match = re.match(r"^([^<]+)", name_email)
        name = name_match.group(1).strip() if name_match else ""

        username_match = re.search(r"\(@([^)]+)\)", name_email)
        username = f"@{username_match.group(1)}" if username_match else ""

        key = (name, username)
        contributors[key] += count

    result = []
    for (name, username), count in contributors.items():
        result.append(
            {
                "count": count,
                "name": name,
                "username": username,
            }
        )

    return result


def write_authors_file(contributors, filename="AUTHORS"):
    today = datetime.now().strftime("%Y-%m-%d")
    git = get_git_command()
    try:
        hash_output = subprocess.check_output(
            [git, "rev-parse", "--short", "HEAD"], encoding="utf-8"
        ).strip()  # nosec
    except subprocess.CalledProcessError as e:
        raise RuntimeError("Failed to get git commit hash.") from e

    with open(filename, "w", encoding="utf-8") as f:
        f.write("# Authors of apache/sedona\n\n")
        f.write(
            f"## The List of Contributors sorted by number of commits (as of {today} {hash_output})\n\n"
        )

        for contributor in contributors:
            count = contributor["count"]
            name = contributor["name"]
            username = contributor["username"]
            line = f"{count:6} {name}"
            if username:
                line += f" {username}"
            f.write(line.rstrip() + "\n")


if __name__ == "__main__":
    contributors = get_contributors()
    contributors.sort(key=lambda x: x["count"], reverse=True)
    write_authors_file(contributors)
    print("AUTHORS file has been generated.")
