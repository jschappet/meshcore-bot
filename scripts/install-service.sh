#!/usr/bin/env bash
# Install the Rust bot as a system service on a Linux host with systemd.
set -euo pipefail

usage() {
    cat <<'HELP'
Usage: ./scripts/install-service.sh [--binary PATH] [--user USER] -- BOT_ARGUMENTS...

Run from the directory containing your bot.json, metrics config, and metrics outbox.
The service runs as your current user and uses that directory for relative paths.
By default, builds the release binary using cargo before installing the service.
--binary PATH uses an already-built Linux binary.
--user USER selects the service account (required when invoked directly as root).

Example:
  ./scripts/install-service.sh -- \
    --port /dev/serial/by-id/YOUR-RADIO --config bot.json \
    --metrics-config metrics.local.json --web-host 0.0.0.0 --web-port 8080

The service is named meshcore-bot-rust and is enabled and started immediately.
HELP
}

fail() { echo "Error: $*" >&2; exit 1; }

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
rust_dir="$(cd -- "$script_dir/.." && pwd)"
working_dir="$(pwd -P)"
service_user="${SUDO_USER:-$(id -un)}"
binary=""
while (($#)); do
    case "$1" in
        --help|-h) usage; exit 0 ;;
        --binary|--user)
            (($# >= 2)) || fail "$1 requires a value"
            if [[ "$1" == --binary ]]; then binary="$2"; else service_user="$2"; fi
            shift 2 ;;
        --) shift; break ;;
        *) fail "Unknown installer option $1. Put bot arguments after --." ;;
    esac
done
(($#)) || { usage; exit 1; }
[[ "$(uname -s)" == Linux ]] || fail "This installer requires Linux with systemd."
command -v systemctl >/dev/null || fail "systemctl is required."
[[ "$service_user" != root ]] || fail "Choose a non-root service user with --user."
id "$service_user" >/dev/null 2>&1 || fail "Unknown user $service_user."

# Validate arguments before making system changes.
for argument in "$working_dir" "$service_user" "$@"; do
    [[ "$argument" != *$'\n'* && "$argument" != *$'\r'* ]] || fail "Arguments must fit on one line."
done
if [[ -z "$binary" ]]; then
    command -v cargo >/dev/null || fail "Install a current Rust toolchain or supply --binary."
    [[ "$(id -u)" != 0 ]] || fail "Build as your regular user, or supply --binary."
    cargo build --release --locked --manifest-path "$rust_dir/Cargo.toml"
    binary="$rust_dir/target/release/meshcore-bot"
fi
[[ -x "$binary" ]] || fail "Executable not found: $binary"

# systemd interprets percent specifiers and dollars separately from quoted arguments.
quote_unit() {
    local value="$1"
    value="${value//\\/\\\\}"
    value="${value//\"/\\\"}"
    value="${value//%/%%}"
    value="${value//\$/\$\$}"
    printf '"%s"' "$value"
}

# WorkingDirectory is a path directive, with different parsing from ExecStart.
escape_directory() {
    local value="$1"
    value="${value//\\/\\\\}"
    value="${value//%/%%}"
    value="${value// /\\x20}"
    value="${value//$'\t'/\\x09}"
    value="${value//\"/\\x22}"
    value="${value//\'/\\x27}"
    printf '%s' "$value"
}

unit_file="$(mktemp)"
trap 'rm -f -- "$unit_file"' EXIT
{
    cat <<UNIT
[Unit]
Description=MeshCore Rust Bot
After=network-online.target
Wants=network-online.target
StartLimitIntervalSec=0

[Service]
Type=simple
User=$service_user
UNIT
    printf 'WorkingDirectory=%s\n' "$(escape_directory "$working_dir")"
    printf 'ExecStart=/usr/local/bin/meshcore-bot-rust'
    for argument in "$@"; do printf ' %s' "$(quote_unit "$argument")"; done
    printf '\n'
    cat <<'UNIT'
Restart=on-failure
RestartSec=10
Environment=RUST_LOG=info
StandardOutput=journal
StandardError=journal
SyslogIdentifier=meshcore-bot-rust
NoNewPrivileges=true
UMask=0077

[Install]
WantedBy=multi-user.target
UNIT
} > "$unit_file"

privilege=()
if [[ "$(id -u)" != 0 ]]; then
    command -v sudo >/dev/null || fail "sudo is required to install the system service."
    privilege=(sudo)
    sudo -v
fi
# The radio device group grants the service the same serial access as an interactive user.
if getent group dialout >/dev/null; then
    "${privilege[@]}" usermod -a -G dialout "$service_user"
fi
"${privilege[@]}" install -o root -g root -m 0755 "$binary" /usr/local/bin/meshcore-bot-rust.new
"${privilege[@]}" mv -f /usr/local/bin/meshcore-bot-rust.new /usr/local/bin/meshcore-bot-rust
"${privilege[@]}" install -o root -g root -m 0644 "$unit_file" /etc/systemd/system/meshcore-bot-rust.service
"${privilege[@]}" systemctl daemon-reload
"${privilege[@]}" systemctl enable meshcore-bot-rust.service
"${privilege[@]}" systemctl restart meshcore-bot-rust.service
"${privilege[@]}" systemctl --no-pager status meshcore-bot-rust.service
printf '\nFollow logs with: journalctl -u meshcore-bot-rust -f\n'
