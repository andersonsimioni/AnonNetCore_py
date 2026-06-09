# Social PoC

The social PoC demonstrates an external application using the local core API. It
is intentionally simple: one local HTML file, local JavaScript, and no required
web server.

## Features

- multiple local social profiles;
- one virtual node per profile;
- profile picture, display name, bio, friends, and posts;
- full profile-state publication through DDT and signed DPT;
- friend feed built by resolving each friend's DPT/DDT state;
- direct messages through virtual sessions;
- WebSocket event handling for incoming messages;
- local cache reset for demos.

## Running

```powershell
.\.venv\Scripts\python.exe scripts\run_poc.py 10
```

The command starts the Docker cluster, local core, Debug Console, and opens the
local HTML PoC unless `--no-open` is used.

## Profile State

The model is:

```text
1 virtual node = 1 social profile
```

The full profile state is stored as content. It includes profile data and feed
posts:

```text
profile + feed_posts + friend_virtual_node_ids
```

The current social profile DPT logical key is:

```text
<virtual_node_id>|profile
```

The physical DHT key is derived by the core as:

```text
sha512("dpt|<virtual_node_id>|profile")
```

The DPT `target_ref` points to the latest content id for that profile state.

## Publication Flow

1. Store the full profile state as local content.
2. Publish in DDT that the local VN holds that content.
3. Publish or update the signed DPT pointer to the latest content id.

The background sync repeats this logic when the local state changes or when the
page needs to refresh DHT visibility.

## Friend Feed Flow

For each friend virtual-node id:

1. Query the friend's DPT using `<friend_virtual_node_id>|profile`.
2. Validate the DPT signature.
3. Read the DPT `target_ref`.
4. Query DDT holders for that content id.
5. Start or reuse a virtual session with a holder.
6. Download the profile state file.
7. Render profile data and posts.

The MVP uses temporal ordering only. There is no ranking algorithm.

## Direct Message Flow

1. The user chooses a friend by VN id.
2. The app asks the core to start a virtual session.
3. The core resolves DRT and DPNT.
4. The virtual session is established over `ROUTE_DATA`.
5. The app sends a virtual application message.
6. The friend receives the message through WebSocket.

## Local Storage

The PoC stores browser-side state in local storage so profiles survive page
reloads. A reset button clears the local site cache for demo purposes.
