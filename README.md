# MeshCore Bot in Rust

Send commands to the bot over MeshCore by direct message. Use `--channel` to
enable replies on one radio slot. Public and `#test` are excluded by name and
by their known keys. Commands are case-insensitive.

- `ping` replies `Pong!`.
- `test [phrase]` or `t [phrase]` acknowledges receipt and shows the incoming
  repeater hop count or direct route, plus SNR when the radio supplies it.
  Example reply for `t checking` is `ACK | 2 hops | SNR -2 dB | checking`.
  The bot echoes up to 48 bytes of the phrase, keeping UTF-8 characters intact.
- `path`, `decode`, or `route` shows repeater hashes when the bot can match
  the channel message to the underlying RF packet. Without a match, it shows
  the route or hop count reported by the companion.
- `prefix AB` looks up repeaters in the radio's contacts whose first key byte
  is `AB`. By default it shows repeaters heard within seven days. Add `all`
  to include older contacts, use `prefix refresh` to refresh the snapshot,
  or use `prefix free` / `prefix available` to sample prefixes unused in
  those contacts.
- `stats` reports bot activity over the last 24 hours. Use `stats messages`,
  `stats channels`, or `stats paths` for more detail. Counters live in
  memory, survive radio reconnects, and reset when the bot process restarts.
- `multitest` or `mt` listens for six seconds for copies of the same channel
  packet and reports the distinct paths it observed.
- `trace 01,7A,55` sends a one-way link trace. `tracer 01,7A,55` mirrors
  the route as `01,7A,55,7A,01` so the bot can hear the return. The reply
  shows SNR by hop and at the bot when it hears a matching trace result.
  Both commands accept an omitted path when the bot can recover the incoming
  channel path, and send a round trip in that case. An explicit path is
  required for a DM. Each command sends one trace with a limit of 12 hops
  after any round-trip expansion.

To try path discovery and multitest, enable channel replies and send the
command live on that channel. These commands need the raw radio frames from
when the message arrived, so a queued message received while the bot was
offline may have only route or hop-count information. The bot uses the channel
key from the radio to match raw frames to queued channel messages and keeps
the key in memory. Long lists are shortened to fit a single MeshCore message.

Connect a radio running MeshCore USB Companion firmware and close other clients
using its serial port. On macOS, find the radio with `ls /dev/cu.*` and use its
USB device path, such as `/dev/cu.usbserial-0001` or `/dev/cu.usbmodem1234`.
On Linux, the port is often `/dev/ttyACM0` or `/dev/ttyUSB0`.
From the repository root, substitute your device path below:

```sh
cargo run --locked --manifest-path Cargo.toml -- --port /dev/cu.usbserial-0001
```

Set `MESHCORE_PORT` to choose the default port, or pass `--port` explicitly.
Use `--channel INDEX` for an eligible test channel other than Public or `#test`.
Channel replies use the radio's current flood scope. The bot drains queued
messages and replies to queued commands on startup and after reconnecting.
If the radio disconnects during operation, the bot retries the same device
path every two seconds. If macOS assigns a different path, restart the bot
with the new path. A reply interrupted by a disconnect is never retried by
the bot. Send a fresh `ping` if you did not get a reply.

## Repeater and antenna field test

1. Leave the bot's radio at a fixed location. Use a second node as the field
   sender and enable replies on the chosen channel with `--channel INDEX`.
   Capture the bot's stderr to keep per-packet results, for example
   `2> antenna-test.log`. Probe results appear at the default `info` level.
2. Start at a known-good point. Send `probe baseline 1/20` through
   `probe baseline 20/20` at a consistent interval, such as 15 seconds
   between sends. Record which numbered ACKs reach the sender. Run
   `probe stats baseline` on the same channel for the unique packet count,
   duplicates, and median signal, or read the bot log if the summary cannot
   get back to you.
3. Repeat from progressively farther points with a fresh run ID for each
   location or setup. Record location, distance, antenna height and
   orientation, radio settings, and whether the repeater's hash appears in
   the recorded path. For an antenna comparison, repeat at the same points
   and keep the other settings fixed.
4. Compare unique packets received at the bot with ACKs received at the
   sender. These measure the outbound leg and the full round trip,
   respectively. Compare signal readings per path. For a relayed packet
   the bot measures the final RF hop. To measure the field-node-to-repeater
   leg directly, take signal readings at the repeater.

A run supports up to 100 numbered packets. The bot keeps up to 32 runs in
memory for two hours of inactivity. Runs survive radio reconnects and reset
when the process restarts. RSSI and exact repeater hashes require a matching
live channel RF frame. DMs can still be used to test delivery and record SNR.

## Web Viewer

Start the bot with `--web-port 8080`, then open
[http://127.0.0.1:8080/](http://127.0.0.1:8080/) on the machine running the bot.
For example:

```sh
cargo run --locked --manifest-path Cargo.toml -- \
  --port /dev/cu.usbserial-0001 --config bot.json --web-port 8080
```

The viewer listens on `127.0.0.1` by default. To open it from another device,
add `--web-host 192.168.1.50`, substituting the bot machine's LAN IP, and
browse to that IP with the configured port. Use `--web-host 0.0.0.0` to
listen on all IPv4 interfaces. The viewer has no login and is readable by
anyone who can reach the listener.

The viewer shows radio status and probe runs with
delivery counts, duplicates, median SNR and RSSI, and individual numbered
receptions. Select a run to see its signal and path details. The page refreshes
every five seconds. It reads the bot's in-memory probe runs, so the same
retention limits apply.

The Actix viewer registers `GET /api/health`, `GET /api/probes`, and
`GET /api/probes/{id}` under a scoped API. The run list returns the numeric ID
used by the detail endpoint.

## Workshop IoT metrics

Pass `--metrics-config metrics.local.json` to send five-minute reports to the
same Workshop IoT endpoint as Flood Monitor. The file uses Flood Monitor's
configuration format:

```json
{
  "url": "https://YOUR-HERON-HOST/api/iot",
  "device_id": "meshcore-bot-rust",
  "name": "MeshCore Bot",
  "ip_address": "127.0.0.1",
  "port": 8080,
  "public_channel": 0,
  "spool_dir": "meshcore-bot-metrics-outbox",
  "root_ca_file": null
}
```

The bot enrolls through the same per-device approval flow and saves its
credential and pending reports in `spool_dir`. Use a separate device ID and
outbox for an independent reporter. The bot counts decoded DMs and received
messages by radio channel slot, including channels where bot replies are
disabled. Counters exclude outgoing replies and messages drained from the
radio's backlog after startup or reconnect.

Neighbor reports list unique repeaters heard directly through signed
zero-hop adverts during the window, with advertised identity, SNR, and RSSI.
The collection is passive. A report includes at most 48 unique repeaters and
marks whether it hit that limit. Reports carry listening time so a partially
offline window is distinguishable from a quiet one. The payload retains
`public_count` and `dm_count` and adds `channel_counts` and `neighbors` in
`meshcore_message_count` schema version 2. Message text and DM sender keys
stay out of the report.

For troubleshooting, start with `RUST_LOG=debug`. Startup logs show the
endpoint, device ID, queue depth, and any remaining retry delay. Request logs
show the report ID, HTTP status, timing, and acknowledgment fields. Transport
errors include their causes at debug level. The device token and request and
response bodies stay out of the logs.

After approving the device in Workshop, add `--metrics-retry-now` on the next
bot start to clear a pending provisional enrollment delay. This keeps the
device credential and queued reports. Server rate limits still apply.

## Automation and admin commands

Save a JSON file such as `bot.json` and pass it with `--config bot.json`.
The following example schedules a daily channel post. Choose the channel slot
on your radio before enabling the schedule.

```json
{
  "admin_pubkeys": [],
  "scheduled_messages": [
    { "channel": 0, "daily_utc": "20:00", "message": "Net starts soon" }
  ]
}
```

Each entry takes either `daily_utc` in 24-hour `HH:MM` format or
`every_seconds` as a positive interval. Messages can contain up to 100 UTF-8
bytes. Set `advert_interval_seconds` to 3600 or more to schedule flood
adverts. Omit it to leave automatic adverts disabled. The bot skips jobs
missed while reconnecting. Scheduled posts use the radio's current flood
scope.

Run with the config file and, if desired, enable channel replies separately:

```sh
cargo run --locked --manifest-path Cargo.toml -- \
  --port /dev/cu.usbserial-0001 --config bot.json --channel 3
```

Add your own node's full 64-character hex public key to `admin_pubkeys` and
make sure your node is in the bot radio's contacts. An empty list leaves
admin commands disabled. The bot accepts admin commands by plain DM when the
sender resolves to exactly one radio contact and its full key is on the list.

| Admin command | Behavior |
| --- | --- |
| `schedule` | Show configured post times and the flood advert interval. |
| `advert` | Request a flood advert. Manual and scheduled adverts share a one-hour cooldown. |
| `hb status` / `hb on` / `hb off` / `hb reset` | Inspect, toggle, or reset the heartbeat counter. `heartbeat` is also accepted. |
| `channelpause` / `channelresume` | Pause or resume channel replies. DMs and scheduled transmissions continue. The pause survives radio reconnects and resets when the process restarts. |
| `reload` | Reload the JSON settings. An invalid file leaves the current settings active. |
| `announcements send 0 Net starts now` | Post a one-off announcement to radio channel slot 0. |
| `repeater status` / `repeater list` | Inspect radio contacts and repeaters. `scan` and `stats` also show the current counts. |

### Heartbeat

Add the following top-level field to your existing `bot.json`:

```json
"heartbeat": {
  "enabled": true,
  "interval_seconds": 300
}
```

With one key in `admin_pubkeys`, that admin receives the heartbeat. With
multiple admins, add `recipient_pubkey` inside `heartbeat` and set it to
the recipient's full key from the admin list. The recipient must be a
unique contact on the bot's radio.

The first numbered heartbeat arrives after one interval. It is sent as a
private DM and uses the radio's normal DM routing. Missed intervals during
a disconnect are skipped. Toggle the heartbeat by admin DM. Restarting or
reloading uses the `enabled` value in the file. To change the interval,
edit `interval_seconds` and send `reload` by admin DM.

Use `hb fast` for 60 seconds, `hb slow` for 60 minutes, or `hb default`
for 300 seconds. Changing the interval starts the new countdown while
preserving the counter and on/off setting. These overrides last until the
next reload or restart. Omitting `interval_seconds` in the file sets it to
300 seconds.

Each DM includes the sequence and the bot's local date, time, and UTC offset,
for example `Heartbeat #12 | 2026-11-15 18:30:00 -08:00`. The counter starts
at #1 on each new day in the bot machine's timezone. Send `hb reset` to start
the counter at #1 on the next scheduled send. The reset preserves the
configured interval and current on/off setting. `hb status` shows today's count
since the last reset.

## Logging

Logs go to stderr and default to `info`. Use `RUST_LOG=debug` to see command
dispatch, admin authorization decisions, scheduled jobs, and whether the
companion accepted outgoing messages:

```sh
RUST_LOG=debug cargo run --locked --manifest-path Cargo.toml -- \
  --port /dev/cu.usbserial-0001 --config bot.json
```

Use `RUST_LOG=trace` for serial troubleshooting. That level adds the opcode
and length of each frame, including idle polls, without printing frame contents.

At `debug` and above, incoming DMs and channel messages log the received text,
with line breaks and other control characters escaped.

Serial framing and the startup handshake are adapted from
[flood_monitor](https://github.com/jschappet/flood_monitor). The local test
simulates a companion over a Unix socket and checks the transmitted reply frames.

```sh
cargo test --locked --manifest-path Cargo.toml
```

### Repeater statistics

Send `rp stats` by admin DM to refresh the radio contact snapshot and get
total repeaters, contacts advertised within 24 hours and seven days, contacts
older than seven days, unknown advert times, location coverage, and the number
of one-byte prefixes shared by multiple repeater contacts. The active windows
overlap. Unknown times are counted separately from stale contacts.

The Web Viewer shows this snapshot and the contact names, prefixes, last advert
times, and coordinates. It also serves `GET /api/repeaters` through the Actix
API scope. Send `rp stats` again to refresh the snapshot. The snapshot lives in
memory across radio reconnects and resets when the bot restarts. These are
radio contact statistics, so they include repeaters reached through the mesh.

Set `tracked_repeaters` in bot.json to an array of full 64-character repeater
public keys. After reloading, `rp stats` reports how many of those targets
have been observed and their activity. The viewer retains all discovered
repeaters. Signed raw adverts are collected even when a repeater is absent
from the radio's saved contacts. Tracking begins when the bot hears an advert
or reads a matching contact. Merely configuring a key does not mark it heard.

### Channel viewer

The dashboard includes channel messages decoded by the companion radio,
including Public and #test. Select a channel slot or view all channels, and
pause or resume the display. It refreshes every five seconds and shows the
received time, text, SNR when available, and route or hop count.

The bot retains the latest 1,000 channel messages in memory across reconnects.
The API returns up to 300 newest messages with
`GET /api/channels?channel=0`; omit the query to view all slots. History resets
on process restart. Messages drained from the radio's startup backlog appear
with the time the bot retrieved them. DMs are excluded from this view.
Anyone who can reach the viewer can read the displayed channel messages.

## Raspberry Pi service

### Build a release

Use a current stable Rust toolchain on the Pi. From the repository root:

```sh
cargo build --release --locked --manifest-path Cargo.toml
```

The executable is `target/release/meshcore-bot`. To run it directly,
stop any existing bot using the serial port, then use your usual arguments:

```sh
./target/release/meshcore-bot \
  --port /dev/ttyACM0 --config bot.json \
  --metrics-config metrics.local.json --web-host 0.0.0.0 --web-port 8080
```

### Install and enable the service

On the Pi, run the installer as your regular user from the directory containing
your bot settings and metrics outbox. Stop the bot running in your terminal
first so the service can open the radio. Substitute the same port and options
you already use:

```sh
./scripts/install-service.sh -- \
  --port /dev/ttyACM0 --config bot.json \
  --metrics-config metrics.local.json --web-host 0.0.0.0 --web-port 8080
```

The script builds the locked release binary, installs it in
`/usr/local/bin/meshcore-bot-rust`, and creates, enables, and starts
`meshcore-bot-rust.service`. It requests sudo for system installation.
The service runs as your user, with the installer's starting directory as its
working directory, so existing relative config paths and device credentials
continue to work. It adds the user to dialout when that group exists.

Use `--binary /path/to/meshcore-bot` before `--` to install a prebuilt Pi
binary instead of building. Keep the working directory and settings files
in place.

### Update the release and restart

After updating the source, build and install the new binary from the repository
root. The service runs the installed copy in `/usr/local/bin`, so rebuilding
alone does not update the running service.

```sh
cargo build --release --locked --manifest-path Cargo.toml
./scripts/install-service.sh --binary target/release/meshcore-bot -- \
  --port /dev/ttyACM0 --config bot.json \
  --metrics-config metrics.local.json --web-host 0.0.0.0 --web-port 8080
```

Use your existing service arguments in the installer command. It regenerates
the unit, reloads systemd, enables the service, and restarts it with the new
binary.

### Restart and check logs

To restart the installed version after changing settings:

```sh
sudo systemctl restart meshcore-bot-rust
systemctl --no-pager status meshcore-bot-rust
journalctl -u meshcore-bot-rust -n 50 --no-pager
```

To follow logs continuously:

```sh
journalctl -u meshcore-bot-rust -f
```

To stop the service and disable startup at boot:

```sh
sudo systemctl disable --now meshcore-bot-rust
```

The service starts at boot and retries failed starts every ten seconds.
To increase logging, use `sudo systemctl edit meshcore-bot-rust` and add:

```ini
[Service]
Environment=RUST_LOG=debug
```

Then restart the service.

### Repeater map

Open `/repeaters/map` or choose **View repeater map** on the dashboard.
The map shows repeaters with valid advertised coordinates, supports name and
prefix search, and can filter to configured tracked repeaters. It refreshes
every ten seconds and reports how many entries lack valid coordinates.
The map uses Leaflet and OpenStreetMap tiles, loaded over the internet.

The list combines radio contacts and signed adverts, including relayed adverts.
A marker indicates an advertised location. It does not establish a direct
link or confirm that the repeater receives your transmissions. Direct-neighbor
metrics separately count signed zero-hop adverts heard by your radio.

Channel labels are read from the radio when a channel message arrives. Sender
keys are offered as contact-name matches in expandable rows. Channel messages
carry no sender public key, so these matches are explicitly unverified and may
include multiple contacts. Contact names and keys refresh on incoming channel
traffic at most once per minute.

### Saved contacts

The bot saves the radio contact snapshot and discovered signed repeater
adverts to `contacts.json` in its working directory. It reads the radio's
contacts on connection and updates the snapshot during later contact queries.
Saved repeater entries are restored into the stats and map after a restart.
Signed adverts create or update local repeater entries by full public key.

Use `--contacts-file /path/to/contacts.json` to select a different file.
The parent directory must exist and be writable by the service user. Writes
use a temporary file and rename, and an exclusive lock prevents two bot
processes using the same database. A damaged database is reported on startup.
These local records do not add entries to the radio's own contact table.
