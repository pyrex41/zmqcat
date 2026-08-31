"""Example AI-harness mailbox loop.

    zmqcat serve --local
    python examples/python/agent.py
    zmqcat put jobs '{"task":"ping"}'
"""

from zmqcat import Client
import json


def main() -> None:
    c = Client(name="agent")
    print("agent waiting on mailbox 'jobs'", flush=True)
    while True:
        msg = c.take("jobs")
        body = msg.get("text") or ""
        print(f"got {body!r} from {msg.get('from')}", flush=True)
        try:
            job = json.loads(body) if body else {}
        except json.JSONDecodeError:
            job = {"task": body}
        result = {"ok": True, "task": job.get("task"), "agent": "agent"}
        c.put("jobs.out", json.dumps(result))
        c.pub("harness.done", json.dumps(result))


if __name__ == "__main__":
    main()
