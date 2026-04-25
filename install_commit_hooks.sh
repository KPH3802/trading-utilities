#!/bin/bash
# Install GMC commit-msg hook in every git repo under our managed roots.
#
# Idempotent: re-run any time. Pre-existing custom commit-msg hooks are
# backed up to commit-msg.pre-gmc-hook.bak before overwrite.
#
# Source-of-truth lives here (trading_utilities). The installed hooks live
# in each repo's .git/hooks/, which is NOT version-controlled, so this
# script must be re-run when new repos appear or after a fresh clone.

set -u

SEARCH_ROOTS=(
    "$HOME/Desktop/Claude_Programs/Trading_Programs"
    "$HOME/Desktop/OptionsVolumeAnalyzer"
)

HOOK_MARKER="GMC_COMMIT_HOOK_v1"

read -r -d '' HOOK_SCRIPT <<'HOOK_EOF'
#!/bin/bash
# GMC_COMMIT_HOOK_v1
# Rejects LLM-attribution trailers in commit messages.
# Installed by trading_utilities/install_commit_hooks.sh

COMMIT_MSG_FILE="$1"

# Patterns to block (case-insensitive). Add new ones here.
PATTERNS=(
    "Co-Authored-By:"
    "🤖 Generated with"
    "Generated with Claude"
)

for pattern in "${PATTERNS[@]}"; do
    if grep -qiF "$pattern" "$COMMIT_MSG_FILE"; then
        offending_line=$(grep -inF "$pattern" "$COMMIT_MSG_FILE" | head -1)
        echo ""
        echo "❌ COMMIT REJECTED — LLM-attribution trailer detected."
        echo ""
        echo "Matched pattern: $pattern"
        echo "Offending line:  $offending_line"
        echo ""
        echo "Per Kevin's repo policy, GMC commits must NOT include:"
        for p in "${PATTERNS[@]}"; do echo "  - $p"; done
        echo ""
        echo "Re-commit with the trailer removed."
        echo ""
        exit 1
    fi
done

exit 0
HOOK_EOF

installed=0
backed_up=0
declare -a BACKUPS=()
declare -a INSTALLED_PATHS=()

for root in "${SEARCH_ROOTS[@]}"; do
    if [ ! -d "$root" ]; then
        echo "[SKIP] root not found: $root"
        continue
    fi
    while IFS= read -r git_dir; do
        repo_dir="$(dirname "$git_dir")"
        repo_name="$(basename "$repo_dir")"
        hook_path="$git_dir/hooks/commit-msg"

        if [ -f "$hook_path" ] && ! grep -q "$HOOK_MARKER" "$hook_path"; then
            backup_path="$hook_path.pre-gmc-hook.bak"
            cp "$hook_path" "$backup_path"
            BACKUPS+=("$repo_name: $backup_path")
            backed_up=$((backed_up + 1))
            echo "[BACKUP] $repo_name: existing hook -> $backup_path"
        fi

        printf '%s\n' "$HOOK_SCRIPT" > "$hook_path"
        chmod +x "$hook_path"
        INSTALLED_PATHS+=("$git_dir")
        installed=$((installed + 1))
        echo "[INSTALLED] $repo_name: $hook_path"
    done < <(find "$root" -maxdepth 3 -type d -name .git 2>/dev/null)
done

echo ""
echo "Installed in $installed repos."
if [ "$backed_up" -gt 0 ]; then
    echo "Backed up $backed_up pre-existing hook(s):"
    for b in "${BACKUPS[@]}"; do echo "  $b"; done
fi

verify_pass=0
verify_fail=0
for git_dir in "${INSTALLED_PATHS[@]}"; do
    hook_path="$git_dir/hooks/commit-msg"
    if [ -x "$hook_path" ] && grep -q "$HOOK_MARKER" "$hook_path"; then
        verify_pass=$((verify_pass + 1))
    else
        verify_fail=$((verify_fail + 1))
        echo "[VERIFY-FAIL] $hook_path"
    fi
done

total=$((verify_pass + verify_fail))
echo "[VERIFY] $verify_pass/$total repos passed."

if [ "$verify_fail" -eq 0 ] && [ "$installed" -gt 0 ]; then
    exit 0
else
    exit 1
fi
