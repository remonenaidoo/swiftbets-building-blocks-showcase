#!/usr/bin/env bash
# Downloads the SwiftBets.* packages pinned in Directory.Packages.props from the public GitHub releases of
# swiftbets-building-blocks-showcase and swiftbets-contracts-showcase into .packages/ (the local feed nuget.config points at).
# No sign-in needed; set GH_TOKEN to lift GitHub's anonymous rate limit.
set -euo pipefail

owner="${SWIFTBETS_OWNER:-remonenaidoo}"
root="${1:-.}"
feed="$root/.packages"
mkdir -p "$feed"

props="$root/Directory.Packages.props"
[[ -f "$props" ]] || { echo "no Directory.Packages.props in $root"; exit 0; }

auth=()
[[ -n "${GH_TOKEN:-}" ]] && auth=(-H "Authorization: Bearer $GH_TOKEN")

{ grep -oE 'Include="SwiftBets\.[A-Za-z.]+" Version="[^"]+"' "$props" || true; } |
  sed -E 's/Include="([^"]+)" Version="([^"]+)"/\1 \2/' |
  while read -r package version; do
    case "$package" in
      SwiftBets.Contracts*) repo="swiftbets-contracts-showcase" ;;
      SwiftBets.BuildingBlocks*) repo="swiftbets-building-blocks-showcase" ;;
      *) echo "unknown shared package $package"; exit 1 ;;
    esac
    echo "$repo v$version"
  done | sort -u |
  while read -r repo tag; do
    echo "fetching $repo $tag"
    release=$(curl -fsSL --retry 3 "${auth[@]}" "https://api.github.com/repos/$owner/$repo/releases/tags/$tag")
    python3 -c 'import json,sys; [print(a["url"], a["name"]) for a in json.load(sys.stdin)["assets"] if a["name"].endswith(".nupkg")]' <<<"$release" |
      while read -r url name; do
        curl -fsSL --retry 3 "${auth[@]}" -H "Accept: application/octet-stream" -o "$feed/$name" "$url"
      done
  done
