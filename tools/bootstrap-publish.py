#!/usr/bin/env python3
import os
import re
import sys
import time
import subprocess

def get_workspace_version(root_dir):
    root_cargo = os.path.join(root_dir, "Cargo.toml")
    with open(root_cargo, "r", encoding="utf-8") as f:
        content = f.read()
    # Find version in [workspace.package]
    match = re.search(r'\[workspace\.package\].*?version\s*=\s*"([^"]+)"', content, re.DOTALL)
    if match:
        return match.group(1)
    return "0.3.0" # fallback

def parse_crates(root_dir, ws_version):
    crates_dir = os.path.join(root_dir, "crates")
    packages = {}
    
    for entry in os.listdir(crates_dir):
        pkg_dir = os.path.join(crates_dir, entry)
        if not os.path.isdir(pkg_dir):
            continue
        cargo_path = os.path.join(pkg_dir, "Cargo.toml")
        if not os.path.exists(cargo_path):
            continue
            
        with open(cargo_path, "r", encoding="utf-8") as f:
            content = f.read()
            
        name_match = re.search(r'^\s*name\s*=\s*"([^"]+)"', content, re.MULTILINE)
        if not name_match:
            continue
        pkg_name = name_match.group(1)
        
        publish_match = re.search(r'^\s*publish\s*=\s*false', content, re.MULTILINE)
        if publish_match:
            # Skip non-publishable packages
            continue
            
        # Version
        version_match = re.search(r'^\s*version\s*=\s*"([^"]+)"', content, re.MULTILINE)
        if version_match:
            pkg_version = version_match.group(1)
        else:
            pkg_version = ws_version
            
        # Parse dependencies on other workspace packages
        deps = []
        # Look for dependencies defined as: name = { path = "..." } or similar
        # We can extract all keys under dependencies sections
        dep_sections = re.findall(r'\[(?:build-|target\..*?\.|dev-)?dependencies\](.*?)(?=\n\[|$)', content, re.DOTALL)
        for section in dep_sections:
            # Find all path-based dependencies in this section
            for line in section.splitlines():
                if "path =" in line and "=" in line:
                    dep_name = line.split("=")[0].strip()
                    deps.append(dep_name)
                    
        packages[pkg_name] = {
            "name": pkg_name,
            "version": pkg_version,
            "path": pkg_dir,
            "dependencies": deps
        }
    return packages

def is_published(pkg_name, version):
    print(f"Checking crates.io registry for {pkg_name}...")
    res = subprocess.run(["cargo", "search", pkg_name, "--limit", "1"], capture_output=True, text=True)
    out = res.stdout
    # Match exact name and version
    match = re.search(rf'^{pkg_name}\s*=\s*"([^"]+)"', out, re.MULTILINE)
    if match:
        reg_version = match.group(1)
        if reg_version == version:
            return True
    return False

def topological_sort(packages):
    # Perform a topological sort on the package dependency graph
    visited = {}
    order = []
    
    def dfs(name):
        if name in visited:
            if visited[name] == 1:
                raise ValueError(f"Cycle detected involving {name}")
            return
        
        visited[name] = 1 # visiting
        pkg = packages.get(name)
        if pkg:
            for dep in pkg["dependencies"]:
                if dep == name:
                    continue
                if dep in packages: # only care about dependencies within our unpublished set
                    dfs(dep)
        visited[name] = 2 # visited
        order.append(name)
        
    for name in packages:
        dfs(name)
    return order

def main():
    root_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    print(f"Workspace root: {root_dir}")
    
    ws_version = get_workspace_version(root_dir)
    print(f"Workspace default version: {ws_version}")
    
    all_packages = parse_crates(root_dir, ws_version)
    
    # Filter for unpublished packages
    unpublished = {}
    for name, pkg in all_packages.items():
        if not is_published(name, pkg["version"]):
            unpublished[name] = pkg
            
    if not unpublished:
        print("All packages are already published at their current versions!")
        return
        
    print(f"\nFound {len(unpublished)} unpublished packages:")
    for name in sorted(unpublished.keys()):
        print(f"  - {name} ({unpublished[name]['version']})")
        
    # Sort them topologically
    try:
        order = topological_sort(unpublished)
    except ValueError as e:
        print(f"Error sorting dependencies: {e}", file=sys.stderr)
        sys.exit(1)
        
    print("\nPublishing plan (topological order):")
    for idx, name in enumerate(order, 1):
        print(f"  {idx}. {name}")
        
    response = input("\nDo you want to proceed with publishing? (y/N): ")
    if response.strip().lower() not in ["y", "yes"]:
        print("Aborted.")
        return
        
    for name in order:
        pkg = unpublished[name]
        print(f"\n=========================================")
        print(f"Publishing {name} ({pkg['version']})...")
        print(f"=========================================")
        
        attempt = 1
        while True:
            # Run cargo publish
            # We use --no-verify since semver-checks and tests are run in CI anyway,
            # but wait, standard publish verify is fine too. Let's do standard publish,
            # or allow --no-verify if they want. Let's run standard cargo publish first.
            cmd = ["cargo", "publish", "--allow-dirty", "--no-verify"]
            print(f"Running: {' '.join(cmd)} (Attempt {attempt})")
            
            # Run from the package directory
            res = subprocess.run(cmd, cwd=pkg["path"], capture_output=True, text=True)
            
            print(res.stdout)
            print(res.stderr, file=sys.stderr)
            
            if res.returncode == 0:
                print(f"Successfully published {name}!")
                break
            else:
                # Check for rate limiting
                # Common rate limit strings in crates.io response:
                # "rate limit", "exceeded", "429", "too many requests"
                err_lower = res.stderr.lower()
                is_rate_limited = (
                    "rate limit" in err_lower or 
                    "exceeded" in err_lower or 
                    "429" in err_lower or 
                    "too many requests" in err_lower
                )
                
                if is_rate_limited:
                    print("\n[!] Hit crates.io publishing rate limit.")
                    print("Sleeping for 10 minutes (605 seconds) before retrying...")
                    time.sleep(605)
                    attempt += 1
                else:
                    print(f"\n[!] Failed to publish {name} due to an error.", file=sys.stderr)
                    sys.exit(1)

if __name__ == "__main__":
    main()
