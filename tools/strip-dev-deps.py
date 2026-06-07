import os

workspace_dir = "."
crates_dir = os.path.join(workspace_dir, "crates")

for root, dirs, files in os.walk(crates_dir):
    for file in files:
        if file == "Cargo.toml":
            path = os.path.join(root, file)
            with open(path, "r", encoding="utf-8") as f:
                content = f.read()
            
            lines = content.split("\n")
            new_lines = []
            in_dev_deps = False
            modified = False
            for line in lines:
                if line.strip() == "[dev-dependencies]":
                    in_dev_deps = True
                    modified = True
                    continue
                if in_dev_deps:
                    if line.strip().startswith("["):
                        in_dev_deps = False
                    else:
                        continue
                new_lines.append(line)
            
            if modified:
                new_content = "\n".join(new_lines)
                with open(path, "w", encoding="utf-8") as f:
                    f.write(new_content)
                print(f"Stripped dev-dependencies from: {path}")
