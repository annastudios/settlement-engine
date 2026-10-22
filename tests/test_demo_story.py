import os
import threading

from settlement import config, dashboard, demo_story, worker
from settlement.demo_story import DemoStory


class ThreadWorker:
    """Stands in for a demo worker process: the real worker loop, in a thread.
    A thread can't be killed mid-payment, so kill() stops it after its
    current payment; the crash itself is tested in test_recovery."""

    def __init__(self, name):
        self.name = name
        self.worker_id = f"test-{name.split()[-1]}"
        self._stop = threading.Event()
        self._thread = threading.Thread(
            target=worker.run,
            kwargs={"worker_id": self.worker_id, "poll_seconds": 0.02, "stop": self._stop},
            daemon=True,
        )
        self._thread.start()

    def kill(self):
        self._stop.set()
        self._thread.join(timeout=10)

    stop = kill


def test_the_whole_story_plays_out_and_balances(client, rail, monkeypatch):
    monkeypatch.setattr(config, "RETRY_BASE_SECONDS", 0)
    story = DemoStory(start_worker=ThreadWorker, speed=0.01)

    story.run()

    status = story.status()
    assert status["error"] is None, status["log"]
    assert status["running"] is False
    assert [act["state"] for act in status["acts"]] == ["done"] * 6
    narration = " ".join(entry["text"] for entry in status["log"])
    assert "Killed demo worker A" in narration
    assert "in the repair queue" in narration
    assert status["result"]["balanced"] is True
    # Left tidy: the network is healthy again.
    assert rail.get("/chaos").json()["failure_rate"] == 0


def test_a_worker_uses_the_name_it_was_given(monkeypatch):
    # The demo finds its workers by name, so a worker it starts must use the
    # name it was given, not one made from its own process id.
    monkeypatch.setenv("WORKER_ID", "demo-worker-A")

    assert worker.default_worker_id() == "demo-worker-A"


def test_the_demo_tells_each_worker_process_its_name(monkeypatch):
    started = {}

    class FakePopen:
        pid = 12345

        def __init__(self, args, env, **kwargs):
            started["args"], started["env"] = args, env

    monkeypatch.setattr(demo_story.subprocess, "Popen", FakePopen)

    handle = demo_story.WorkerProcess("demo worker A")

    assert started["args"][-2:] == ["-m", "settlement.worker"]
    assert started["env"]["WORKER_ID"] == handle.worker_id
    assert "demo-worker-A" in handle.worker_id


def test_without_a_given_name_a_worker_names_itself(monkeypatch):
    monkeypatch.delenv("WORKER_ID", raising=False)

    assert worker.default_worker_id().endswith(f"-{os.getpid()}")


def test_status_before_the_demo_has_run(client):
    status = client.get("/demo/run").json()

    assert status["running"] is False
    assert [act["state"] for act in status["acts"]] == ["next"] * 6
    assert status["acts"][2]["title"] == "A worker crashes mid-payment"
    assert status["log"] == []


def test_only_one_demo_at_a_time_and_it_can_be_stopped(client, monkeypatch):
    story = DemoStory(start_worker=ThreadWorker)
    released = threading.Event()

    def pretend_to_run():
        released.wait(timeout=5)
        with story._lock:
            story._state["running"] = False

    monkeypatch.setattr(story, "run", pretend_to_run)
    monkeypatch.setattr(dashboard, "story", story)

    assert client.post("/demo/run").status_code == 202
    assert client.get("/demo/run").json()["running"] is True
    assert client.post("/demo/run").status_code == 409

    client.post("/demo/run/stop")
    released.set()
    story._thread.join(timeout=5)
    assert client.get("/demo/run").json()["running"] is False


def test_stopping_mid_story_cleans_up(client, rail, monkeypatch):
    monkeypatch.setattr(config, "RETRY_BASE_SECONDS", 0)
    story = DemoStory(start_worker=ThreadWorker, speed=1)  # real pauses: we stop it first
    started = []
    original_start = story._start_worker

    def start_worker(name):
        handle = original_start(name)
        started.append(handle)
        if len(started) == 2:
            story.stop()  # stop as soon as both workers are up
        return handle

    story._start_worker = start_worker
    story.run()

    status = story.status()
    assert status["running"] is False
    assert status["log"][-1]["text"] == "Demo stopped."
    assert all(not w._thread.is_alive() for w in started)  # its workers are stopped
    assert rail.get("/chaos").json()["failure_rate"] == 0
