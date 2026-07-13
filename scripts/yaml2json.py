#!/usr/bin/env python3
"""Convert cc-switch style provider.yaml to Gateway provider.json.
Usage: python yaml2json.py provider.yaml > provider.json
"""
import json
import sys
import os

try:
    import yaml
except ImportError:
    print("error: pyyaml not installed. Run: pip install pyyaml", file=sys.stderr)
    sys.exit(1)

def main():
    path = sys.argv[1] if len(sys.argv) > 1 else "provider.yaml"
    if not os.path.exists(path):
        print(f"error: {path} not found", file=sys.stderr)
        sys.exit(1)

    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)

    # Expand ${ENV_VAR} references in api_key fields.
    def expand_env(value):
        if isinstance(value, str) and value.startswith("${") and value.endswith("}"):
            return os.getenv(value[2:-1], value)
        return value

    for p in data.get("providers", []):
        if "api_key" in p:
            p["api_key"] = expand_env(p["api_key"])

    json.dump(data, sys.stdout, indent=2, ensure_ascii=False)
    print()

if __name__ == "__main__":
    main()
