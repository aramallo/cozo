# cozo workspace tasks. Run `just` to list recipes.

# Crates whose version is NOT kept in lockstep with the workspace version and
# must never be touched by `set-version`.
#   cozorocks            -> 0.1.x (independent)
#   cozo-core-examples   -> 0.1.x own version (but its `cozo` dep pin IS bumped)

_default:
    @just --list

# Print the current workspace version.
version:
    @cat VERSION

# Set the workspace version everywhere: the VERSION marker file, every lockstep
# crate's own `[package] version`, and every inter-crate `cozo = { version=... }`
# pin. Keyed on the CURRENT version string (read from VERSION) so cozorocks,
# the examples crate's own version, and all third-party dependency versions are
# left untouched. Cargo.lock is re-synced for the workspace members.
#
#   just set-version 0.8.8
set-version new:
    #!/usr/bin/env bash
    set -euo pipefail
    old="$(cat VERSION)"
    new="{{new}}"

    if ! [[ "$new" =~ ^[0-9]+\.[0-9]+\.[0-9]+([.\-+].*)?$ ]]; then
        echo "error: '$new' does not look like a version (expected e.g. 0.8.8)" >&2
        exit 1
    fi
    if [[ "$old" == "$new" ]]; then
        echo "already at $new; nothing to do"
        exit 0
    fi

    echo "Bumping $old -> $new"

    # Replace `version = "<old>"` (own version AND `cozo = { version = "<old>" }`
    # pins) only in files that actually reference the old version. The exact-
    # string match leaves cozorocks (0.1.x) and other deps alone.
    files="$(grep -rl "version = \"$old\"" --include=Cargo.toml --exclude-dir=target --exclude-dir=.git . || true)"
    if [[ -z "$files" ]]; then
        echo "warning: no Cargo.toml referenced version \"$old\"" >&2
    fi
    for f in $files; do
        old="$old" new="$new" perl -i -pe 's/\Qversion = "$ENV{old}"\E/version = "$ENV{new}"/g' "$f"
        echo "  $f"
    done

    # VERSION marker (no trailing newline, matching the original).
    printf '%s' "$new" > VERSION
    echo "  VERSION"

    # Re-sync only the workspace members in Cargo.lock (no network, leaves
    # third-party pins untouched). Non-fatal: the next build also fixes it.
    cargo update --workspace --quiet 2>/dev/null \
        || echo "  (run a build to refresh Cargo.lock)"

    echo "Done. Review with: git diff --stat"

# Create the leapsight release tag for the current VERSION on the current commit.
# The cozodb NIF resolves cozo by this git tag, so tag the commit you want it to
# pick up. Pass `force=true` to move an existing tag.
tag force="false":
    #!/usr/bin/env bash
    set -euo pipefail
    ver="$(cat VERSION)"
    tag="v${ver}-leapsight"
    if [[ "{{force}}" == "true" ]]; then
        git tag -f "$tag"
        echo "moved tag $tag -> $(git rev-parse --short HEAD)"
    else
        git tag "$tag"
        echo "created tag $tag -> $(git rev-parse --short HEAD)"
    fi
    echo "push it with: git push origin $tag" "${force:+--force}"
