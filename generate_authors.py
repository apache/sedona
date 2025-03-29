import subprocess
import re
from datetime import datetime

def get_contributors():
    output = subprocess.check_output(["git", "shortlog", "-sne", "--all"], encoding="utf-8")
    lines = output.strip().split("\n")
    contributors = []
    
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
        
        email_match = re.search(r"<([^>]+)>", name_email)
        email = email_match.group(1) if email_match else ""
        
        username_match = re.search(r"\(@([^)]+)\)", name_email)
        username = f"@{username_match.group(1)}" if username_match else ""
        
        special_mark = ""
        if username and email:
            if email.lower().endswith(("@gmail.com")):
                special_mark += "*"
            elif not name.lower() in email.lower():
                special_mark += "+"
        
        contributors.append({
            "count": count,
            "name": name,
            "username": username,
            "special_mark": special_mark
        })
    
    return contributors

def write_authors_file(contributors, filename="AUTHORS"):
    today = datetime.now().strftime("%Y-%m-%d")
    hash_output = subprocess.check_output(["git", "rev-parse", "--short", "HEAD"], encoding="utf-8").strip()
    
    with open(filename, "w", encoding="utf-8") as f:
        f.write("# Authors of apache/sedona\n\n")
        f.write(f"## The List of Contributors sorted by number of commits (as of {today} {hash_output})\n\n")
        
        for contributor in contributors:
            count = contributor["count"]
            name = contributor["name"]
            username = contributor["username"]
            mark = contributor["special_mark"]
            
            line = f"{count:6} {name} {username}{mark}\n"
            f.write(line)
        
        # f.write("\n`*` - Entries unified according to names and addresses\n")
        # f.write("`+` - Entries with names different from commits\n\n")
        
        # f.write("## Contributors without named commits\n\n")
        # f.write("   Yuichi Osawa (Mitsubishi Electric Micro-Computer Application Software)\n")
        # f.write("   Shota Nakano (Manycolors)\n")
        # f.write("   Bjorn De Meyer\n\n")
        
        # f.write("## Corporate contributors\n\n")
        # f.write("   Ministry of Economy, Trade and Industry, Japan\n")
        # f.write("   Kyushu Bureau of Economy, Trade and Industry\n")
        # f.write("   SCSK KYUSHU CORPORATION\n")
        # f.write("   Kyushu Institute of Technology\n")
        # f.write("   Network Applied Communication Laboratory, Inc.\n")
        # f.write("   Internet Initiative Japan Inc.\n")
        # f.write("   Specified non-profit organization mruby Forum\n")
        # f.write("   Mitsubishi Electric Micro-Computer Application Software Co.,Ltd.\n")
        # f.write("   Manycolors, Inc.\n")

if __name__ == "__main__":
    contributors = get_contributors()
    
    contributors.sort(key=lambda x: x["count"], reverse=True)
    
    write_authors_file(contributors)
    print("AUTHORS file has been generated in the requested format.")