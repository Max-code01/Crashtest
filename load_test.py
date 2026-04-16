import asyncio
import aiohttp
import async_timeout
import time
import logging
import paramiko
import json
import argparse
from pathlib import Path

DEFAULT_TARGET = "http://127.0.0.1:8080/"
DEFAULT_CONCURRENCY = 50
DEFAULT_TOTAL = 2000
DEFAULT_TIMEOUT = 5
DEFAULT_MAX_DURATION = 300
CPU_THRESHOLD = 85.0
MEM_THRESHOLD = 90.0
RESOURCE_CHECK_INTERVAL = 5

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("loadtest")

def percentile(data, p):
    if not data:
        return 0
    data = sorted(data)
    k = (len(data)-1) * (p/100)
    f = int(k)
    c = min(f+1, len(data)-1)
    if f == c:
        return data[int(k)]
    d0 = data[f] * (c-k)
    d1 = data[c] * (k-f)
    return d0 + d1

async def check_remote_resources(ssh_host, ssh_user, ssh_key_path):
    if not ssh_host:
        return None
    try:
        key = None
        if Path(ssh_key_path).exists():
            try:
                key = paramiko.Ed25519Key.from_private_key_file(ssh_key_path)
            except Exception:
                key = paramiko.RSAKey.from_private_key_file(ssh_key_path)
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(ssh_host, username=ssh_user, pkey=key, timeout=5)
        cmd = "python3 -c \"import psutil,json; print(json.dumps({'cpu': psutil.cpu_percent(interval=1),'mem': psutil.virtual_memory().percent}))\""
        stdin, stdout, stderr = client.exec_command(cmd)
        out = stdout.read().decode().strip()
        client.close()
        return json.loads(out)
    except Exception as e:
        logger.debug("Remote resource check failed: %s", e)
        return None

async def worker(name, session, sem, results, stop_event, target, timeout):
    while not stop_event.is_set():
        try:
            async with sem:
                start = time.time()
                status = None
                try:
                    with async_timeout.timeout(timeout):
                        async with session.get(target) as resp:
                            await resp.text()
                            status = resp.status
                except Exception as e:
                    logger.debug("Request error: %s", e)
                elapsed = time.time() - start
                results.append((status, elapsed))
        except asyncio.CancelledError:
            return

async def monitor(stop_event, start_time, ssh_host, ssh_user, ssh_key_path, max_duration):
    while not stop_event.is_set():
        await asyncio.sleep(RESOURCE_CHECK_INTERVAL)
        res = await check_remote_resources(ssh_host, ssh_user, ssh_key_path)
        if res:
            cpu = res.get("cpu",0)
            mem = res.get("mem",0)
            logger.info("Remote CPU %.1f%% MEM %.1f%%", cpu, mem)
            if cpu >= CPU_THRESHOLD or mem >= MEM_THRESHOLD:
                logger.warning("Resource threshold exceeded -> aborting test")
                stop_event.set()
        if time.time() - start_time > max_duration:
            logger.warning("Max duration reached -> aborting")
            stop_event.set()

async def main(args):
    sem = asyncio.Semaphore(args.concurrency)
    results = []
    stop_event = asyncio.Event()
    start_time = time.time()

    async with aiohttp.ClientSession() as session:
        workers = [asyncio.create_task(worker(f"w{i}", session, sem, results, stop_event, args.target, args.timeout))
                   for i in range(args.concurrency)]
        monitor_task = asyncio.create_task(monitor(stop_event, start_time, args.ssh_host, args.ssh_user, args.ssh_key, args.max_duration))

        while not stop_event.is_set() and len(results) < args.total:
            await asyncio.sleep(0.5)

        stop_event.set()
        for w in workers:
            w.cancel()
        await asyncio.gather(*workers, return_exceptions=True)
        await monitor_task

    statuses = {}
    latencies = [r[1] for r in results if r[0] is not None]
    for s,e in results:
        statuses[s] = statuses.get(s,0)+1
    logger.info("Requests: sent=%d successful=%d", len(results), len(latencies))
    if latencies:
        logger.info("Latency ms: p50=%.1f p95=%.1f p99=%.1f",
                    1000 * percentile(latencies,50),
                    1000 * percentile(latencies,95),
                    1000 * percentile(latencies,99))
    logger.info("Status counts: %s", statuses)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Controlled load tester")
    parser.add_argument("--target", default=DEFAULT_TARGET)
    parser.add_argument("--concurrency", type=int, default=DEFAULT_CONCURRENCY)
    parser.add_argument("--total", type=int, default=DEFAULT_TOTAL)
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT)
    parser.add_argument("--max-duration", dest="max_duration", type=int, default=DEFAULT_MAX_DURATION)
    parser.add_argument("--ssh-host", default=None, help="Optional: VM IP for resource checks")
    parser.add_argument("--ssh-user", default=None)
    parser.add_argument("--ssh-key", default=str(Path.home() / ".ssh" / "id_ed25519"))
    args = parser.parse_args()
    asyncio.run(main(args))
