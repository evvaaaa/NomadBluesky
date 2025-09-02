# Import bluesky and ophyd
import os

from bluesky import RunEngine
from bluesky import plan_stubs as bps
from ophyd_async.core import init_devices
from ophyd_async.epics import demo, testing

from nomad_bluesky_callback import NomadCallback

NOMAD_API_URL = os.environ["NOMAD_API_URL"]
NOMAD_API_TOKEN = os.environ["NOMAD_API_TOKEN"]

nomad_callback = NomadCallback(NOMAD_API_URL, NOMAD_API_TOKEN)
nomad_callback.serve()


RE = RunEngine(call_returns_result=True)
RE.subscribe(nomad_callback)

prefix = testing.generate_random_pv_prefix()
ioc = demo.start_ioc_subprocess(prefix, num_channels=3)

with init_devices():
    stage = demo.DemoStage(f"{prefix}STAGE:")
    pdet = demo.DemoPointDetector(f"{prefix}DET:", num_channels=3)


def plan():
    yield from bps.open_run()
    yield from bps.trigger_and_read([stage, pdet])
    yield from bps.trigger_and_read([stage, pdet])
    yield from bps.trigger_and_read([stage, pdet])
    yield from bps.trigger_and_read([stage, pdet])
    yield from bps.close_run()


RE(plan())

# Necessary so that the thread doesn't die before it has a chance to process all the documents.
# Use
nomad_callback.join()
