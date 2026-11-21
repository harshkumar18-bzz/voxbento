import asyncio
import contextlib
import json
import logging
import secrets
import socket
import subprocess
import sys
from datetime import timedelta
from pathlib import Path

ROOT = Path('/home/harsh/.graphify/repos/fossasia/voxbento')
OUT = Path(__file__).parent
sys.path.insert(0, str(ROOT))

def free_port():
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        return sock.getsockname()[1]

REDIS_PORT = free_port()
SMTP_PORT = free_port()
HTTP_PORT = free_port()
from cryptography.fernet import Fernet
from portal import config
config.settings = config.Settings(
    _env_file=None, debug=True, secret_key=secrets.token_hex(32),
    api_key_encryption_key=Fernet.generate_key().decode(),
    database_url=f'sqlite+aiosqlite:///{OUT / "runtime.db"}',
    job_queue_backend='redis', redis_url=f'redis://127.0.0.1:{REDIS_PORT}/0',
    job_queue_reconcile_seconds=60, booth_state_recovery_enabled=False,
    smtp_host='127.0.0.1', smtp_port=SMTP_PORT,
    smtp_from_email='runtime@example.com', smtp_user='', smtp_password='',
)
from portal.config import settings
from portal.database import configure, init_db, get_session, dispose, _get_engine
from portal.job_queue import job_queue
from portal.models import EmailDelivery, utc_now
from portal.notifications import queue_notification, notification_worker_loop
from portal.webhooks.worker import webhook_worker_loop
from sqlalchemy import event

messages = []
async def smtp(reader, writer):
    writer.write(b'220 localhost runtime SMTP\r\n')
    await writer.drain()
    try:
        while line := await reader.readline():
            command = line.decode().strip().upper()
            if command.startswith('EHLO'):
                writer.write(b'250-localhost\r\n250 SIZE 10000000\r\n')
            elif command == 'DATA':
                writer.write(b'354 Send data\r\n')
                await writer.drain()
                body = []
                while (part := await reader.readline()) not in (b'.\r\n', b''):
                    body.append(part)
                messages.append(b''.join(body))
                writer.write(b'250 Accepted locally\r\n')
            elif command == 'QUIT':
                writer.write(b'221 Bye\r\n')
                await writer.drain()
                break
            else:
                writer.write(b'250 OK\r\n')
            await writer.drain()
    finally:
        writer.close()
        await writer.wait_closed()

def start_redis():
    return subprocess.Popen([
        str(OUT / 'redis-7.4.0/src/redis-server'), '--bind', '127.0.0.1',
        '--port', str(REDIS_PORT), '--save', '', '--appendonly', 'no',
        '--dir', str(OUT), '--logfile', str(OUT / 'redis.log'),
    ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

async def redis_ready():
    for _ in range(100):
        try:
            if await job_queue.client().ping():
                return
        except Exception:
            await asyncio.sleep(.1)
    raise RuntimeError('Redis failed to start')

async def cancel(task):
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task

async def queued(**kw):
    async with get_session() as session:
        delivery = await queue_notification(session, 'verification', 'proof@example.com',
            {'verification_link': 'https://example.invalid/local-proof'}, **kw)
        return delivery.id

async def accepted(delivery_id):
    for _ in range(80):
        async with get_session() as session:
            delivery = await session.get(EmailDelivery, delivery_id)
            if delivery.status == 'accepted':
                return
            assert delivery.status not in ('failed', 'disabled'), (delivery.status, delivery.last_error)
        await asyncio.sleep(.1)
    raise AssertionError('Email not accepted')

async def main():
    logging.basicConfig(level=logging.WARNING)
    configure(settings.database_url)
    await init_db()
    redis_process = start_redis()
    server = await asyncio.start_server(smtp, '127.0.0.1', SMTP_PORT)
    results = {'commit': '32108b6', 'environment': 'Native Redis 7.4.0 + SQLite; isolated loopback SMTP',
               'measurement': 'Idle webhook worker, 5 seconds per backend after 1.2-second warmup', 'checks': {}}
    try:
        await redis_ready()
        count = 0
        counting = False
        def sql_count(conn, cursor, statement, parameters, context, executemany):
            nonlocal count
            if counting and 'webhook_deliveries' in statement.lower():
                count += 1
        event.listen(_get_engine().sync_engine, 'before_cursor_execute', sql_count)
        for backend in ('db', 'redis'):
            settings.job_queue_backend = backend
            worker = asyncio.create_task(webhook_worker_loop())
            await asyncio.sleep(1.2)
            count = 0
            counting = True
            await asyncio.sleep(5)
            counting = False
            await cancel(worker)
            results[f'{backend}_idle_webhook_sql_statements'] = count
        assert results['db_idle_webhook_sql_statements'] > 0
        assert results['redis_idle_webhook_sql_statements'] == 0
        settings.job_queue_backend = 'redis'

        # Only the committed transaction may publish its hint.
        async with get_session() as session:
            delivery = await queue_notification(session, 'verification', 'proof@example.com',
                {'verification_link': 'https://example.invalid/local-proof'})
            delivery_id = delivery.id
            assert await job_queue.client().zcard(job_queue.key('email')) == 0
        assert await job_queue.client().zcard(job_queue.key('email')) == 1
        results['checks']['Post-commit enqueue'] = 'PASS'
        try:
            async with get_session() as session:
                await queue_notification(session, 'verification', 'proof@example.com',
                    {'verification_link': 'https://example.invalid/rollback'})
                raise ValueError('Intentional rollback')
        except ValueError:
            pass
        assert await job_queue.client().zcard(job_queue.key('email')) == 1
        results['checks']['Rollback discards hint'] = 'PASS'

        # Lose Redis memory, then run the real email worker against the durable outbox.
        redis_process.terminate()
        redis_process.wait(timeout=5)
        await job_queue.close()
        redis_process = start_redis()
        await redis_ready()
        assert await job_queue.client().zcard(job_queue.key('email')) == 0
        worker = asyncio.create_task(notification_worker_loop())
        await accepted(delivery_id)
        await cancel(worker)
        assert len(messages) == 1
        results['checks']['Redis restart recovery + SMTP delivery'] = 'PASS'

        scheduled = await queued(scheduled_at=utc_now() + timedelta(seconds=2))
        worker = asyncio.create_task(notification_worker_loop())
        await asyncio.sleep(.7)
        assert len(messages) == 1
        await accepted(scheduled)
        await cancel(worker)
        assert len(messages) == 2
        results['checks']['Scheduled delivery waits until due'] = 'PASS'

        redis_process.terminate()
        redis_process.wait(timeout=5)
        fallback = await queued()
        assert await job_queue.ready('email') is None
        worker = asyncio.create_task(notification_worker_loop())
        await accepted(fallback)
        await cancel(worker)
        assert len(messages) == 3
        results['checks']['Redis outage + database fallback delivery'] = 'PASS'

        # Start the actual portal with its normal lifespan and an isolated demo manifest.
        redis_process = start_redis()
        await job_queue.close()
        await redis_ready()
        from portal.tts import demo_gen
        demo_gen.DEMO_DIR = OUT / 'demo'
        demo_gen.DEMO_DIR.mkdir(exist_ok=True)
        demo_gen.MANIFEST_PATH = demo_gen.DEMO_DIR / 'manifest.json'
        demo_gen.MANIFEST_PATH.write_text('{"languages": []}')
        import uvicorn
        import httpx
        from fastapi_app import app
        uvserver = uvicorn.Server(uvicorn.Config(app, host='127.0.0.1', port=HTTP_PORT, log_level='warning'))
        portal_task = asyncio.create_task(uvserver.serve())
        try:
            for _ in range(100):
                if uvserver.started:
                    break
                if portal_task.done():
                    await portal_task
                    raise RuntimeError('Portal failed to start')
                await asyncio.sleep(.1)
            async with httpx.AsyncClient() as client:
                response = await client.get(f'http://127.0.0.1:{HTTP_PORT}/')
                assert response.status_code == 200, response.status_code
                results['checks']['Local portal HTTP 200'] = 'PASS'
        finally:
            uvserver.should_exit = True
            await portal_task
        results['smtp_messages_accepted'] = len(messages)
        results['scope'] = 'Backend runtime evidence; no UI change. Media stack and paid AI providers not exercised.'
        (OUT / 'results.json').write_text(json.dumps(results, indent=2) + '\n')
        print(json.dumps(results, indent=2))
    finally:
        if redis_process.poll() is None:
            redis_process.terminate()
            redis_process.wait(timeout=5)
        server.close()
        await server.wait_closed()
        await job_queue.close()
        await dispose()

asyncio.run(main())
