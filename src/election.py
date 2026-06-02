import os
import threading
import time
import requests
from typing import Optional

NODE_ID: int = int(os.environ.get("NODE_ID", "1"))
ELECTION_TIMEOUT: float = float(os.environ.get("ELECTION_TIMEOUT", "3"))


def _parse_peers() -> dict[int, str]:
    """Parse PEERS env var: "2:http://node-2:8080,3:http://node-3:8080" """
    raw = os.environ.get("PEERS", "")
    result: dict[int, str] = {}
    for item in raw.split(","):
        item = item.strip()
        if not item:
            continue
        node_id_str, url = item.split(":", 1)
        result[int(node_id_str)] = url.strip()
    return result


PEERS: dict[int, str] = _parse_peers()

current_leader: Optional[int] = None
_election_in_progress: bool = False
_lock = threading.Lock()


def start_election() -> None:
    global _election_in_progress, current_leader

    with _lock:
        if _election_in_progress:
            return
        _election_in_progress = True

    higher = {nid: url for nid, url in PEERS.items() if nid > NODE_ID}
    got_ok = False

    for _, url in higher.items():
        try:
            r = requests.post(f"{url}/api/election", json={"node_id": NODE_ID}, timeout=2)
            if r.status_code == 200:
                got_ok = True
        except Exception:
            pass

    if not got_ok:
        declare_victory()
    else:
        # Wait up to ELECTION_TIMEOUT for a COORDINATOR message
        deadline = time.time() + ELECTION_TIMEOUT
        while time.time() < deadline:
            with _lock:
                if current_leader is not None:
                    break
            time.sleep(0.2)
        else:
            declare_victory()

    with _lock:
        _election_in_progress = False


def handle_election_message(sender_id: int) -> None:
    # Caller already returns HTTP 200 (OK) to sender_id
    # This node has a higher ID, so it takes over the election
    threading.Thread(target=start_election, daemon=True).start()


def declare_victory() -> None:
    global current_leader

    with _lock:
        current_leader = NODE_ID

    for _, url in PEERS.items():
        try:
            requests.post(f"{url}/api/coordinator", json={"node_id": NODE_ID}, timeout=2)
        except Exception:
            pass


def set_leader(leader_id: int) -> None:
    global current_leader, _election_in_progress

    with _lock:
        current_leader = leader_id
        _election_in_progress = False


def heartbeat_check() -> None:
    global current_leader

    while True:
        time.sleep(5)

        with _lock:
            leader = current_leader

        if leader is None:
            threading.Thread(target=start_election, daemon=True).start()
            time.sleep(ELECTION_TIMEOUT + 1)
            continue

        if leader == NODE_ID:
            continue

        leader_url = PEERS.get(leader)
        if not leader_url:
            continue

        try:
            r = requests.get(f"{leader_url}/health", timeout=2)
            if r.status_code != 200:
                raise ConnectionError("unhealthy")
        except Exception:
            with _lock:
                current_leader = None
            threading.Thread(target=start_election, daemon=True).start()
            time.sleep(ELECTION_TIMEOUT + 1)
