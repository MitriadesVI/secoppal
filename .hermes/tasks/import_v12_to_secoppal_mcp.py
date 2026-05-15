#!/usr/bin/env python3
from __future__ import annotations

import json
from pathlib import Path

import anyio
from mcp.client.session import ClientSession
from mcp.client.stdio import StdioServerParameters, stdio_client

PROFILE_ROOT = Path('/Users/rodrigoortiz/.hermes/profiles/secoppal')
SERVER_PATH = PROFILE_ROOT / 'memory_db' / 'mcp_server.py'
PYTHON_BIN = Path('/Users/rodrigoortiz/.hermes/.venv/bin/python3')
WORKSPACE_PATH = Path('/Users/rodrigoortiz/Documents/secoppal')
PLAN_JSON = Path('/Users/rodrigoortiz/Documents/secoppal/.hermes/tasks/v1.2-plan.json')
PLAN_MD = Path('/Users/rodrigoortiz/Documents/2026-05-12-secoppal-v1.2-asesor-conversacional.md')


def _extract_json(result) -> dict:
    texts = []
    for item in result.content:
        text = getattr(item, 'text', None)
        if text:
            texts.append(text)
    if not texts:
        raise RuntimeError(f'Respuesta MCP vacía: {result}')
    payload = json.loads(texts[0])
    if getattr(result, 'isError', False):
        raise RuntimeError(f'MCP isError=True: {payload}')
    if isinstance(payload, dict) and payload.get('error'):
        raise RuntimeError(f"MCP error lógico: {payload}")
    return payload


async def _call(session: ClientSession, name: str, arguments: dict) -> dict:
    result = await session.call_tool(name, arguments)
    return _extract_json(result)


def _task_description(task: dict) -> str:
    deps = ', '.join(task.get('dependencies') or []) or 'ninguna'
    core = 'core' if task.get('core') else 'nice-to-have'
    files = ', '.join(task.get('files') or []) or 'sin archivos declarados'
    return (
        f"Plan v1.2 item {task['id']}. Semana {task['week']}. "
        f"Estimación: {task['hours']}h. Tipo: {core}. Riesgo: {task['risk']}. "
        f"Dependencias: {deps}. Archivos: {files}. "
        f"Criterio de done: {task['done_criteria']}"
    )


async def main() -> None:
    data = json.loads(PLAN_JSON.read_text())
    server = StdioServerParameters(
        command=str(PYTHON_BIN),
        args=[str(SERVER_PATH)],
        cwd=str(PROFILE_ROOT),
        env={'PYTHONUNBUFFERED': '1'},
    )

    async with stdio_client(server) as (read_stream, write_stream):
        async with ClientSession(read_stream, write_stream) as session:
            await session.initialize()

            bootstrap = await _call(
                session,
                'session_bootstrap',
                {'workspace_path': str(WORKSPACE_PATH)},
            )
            repo_id = bootstrap['repo']['id']
            print(f'REPO_ID={repo_id}')

            master_task = await _call(
                session,
                'memory_write',
                {
                    'idempotency_key': 'secoppal-v12-master-task',
                    'kind': 'task',
                    'payload': {
                        'repo_id': repo_id,
                        'title': 'SECOPPAL v1.2 — roadmap asesor conversacional',
                        'description': (
                            'Roadmap maestro importado desde v1.2-plan.json. '
                            'Objetivo: demo conversacional por Telegram con 3 momentos cognitivos: '
                            'encontrar, descubrir y explorar. '
                            'Referencias canónicas: '
                            f'{PLAN_JSON} | {PLAN_MD}'
                        ),
                        'priority': 'high',
                        'source': 'user',
                        'status': 'pending',
                        'tags': ['secoppal', 'v1.2', 'roadmap', 'telegram', 'asesor-conversacional'],
                    },
                },
            )
            master_task_id = master_task['result']
            print(f'MASTER_TASK_ID={master_task_id}')

            master_plan = await _call(
                session,
                'memory_write',
                {
                    'idempotency_key': 'secoppal-v12-master-plan',
                    'kind': 'plan',
                    'payload': {
                        'task_id': master_task_id,
                        'description': 'Plan maestro SECOPPAL v1.2 importado desde JSON canónico',
                        'steps': [f"{t['id']} — {t['title']}" for t in data['tasks']],
                        'status': 'active',
                        'tags': ['secoppal', 'v1.2', 'roadmap', 'master-plan'],
                    },
                },
            )
            master_plan_id = master_plan['result']
            print(f'MASTER_PLAN_ID={master_plan_id}')

            artifact_md = await _call(
                session,
                'memory_write',
                {
                    'idempotency_key': 'secoppal-v12-plan-artifact-md',
                    'kind': 'artifact',
                    'payload': {
                        'type_': 'doc',
                        'title': 'SECOPPAL v1.2 — plan canónico (.md)',
                        'file_path': str(PLAN_MD),
                        'linked_entity_type': 'task',
                        'linked_entity_id': master_task_id,
                    },
                },
            )
            print(f'ARTIFACT_MD_ID={artifact_md["result"]}')

            artifact_json = await _call(
                session,
                'memory_write',
                {
                    'idempotency_key': 'secoppal-v12-plan-artifact-json',
                    'kind': 'artifact',
                    'payload': {
                        'type_': 'doc',
                        'title': 'SECOPPAL v1.2 — manifiesto de tareas (.json)',
                        'file_path': str(PLAN_JSON),
                        'linked_entity_type': 'task',
                        'linked_entity_id': master_task_id,
                    },
                },
            )
            print(f'ARTIFACT_JSON_ID={artifact_json["result"]}')

            created = []
            for task in data['tasks']:
                priority = 'high' if task.get('core') else 'medium'
                tags = [
                    'secoppal',
                    'v1.2',
                    f"week-{task['week']}",
                    'core' if task.get('core') else 'nice-to-have',
                    f"task-id-{task['id']}",
                ]
                resp = await _call(
                    session,
                    'memory_write',
                    {
                        'idempotency_key': f"secoppal-v12-task-{task['id']}",
                        'kind': 'task',
                        'payload': {
                            'repo_id': repo_id,
                            'title': f"[{task['id']}] {task['title']}",
                            'description': _task_description(task),
                            'priority': priority,
                            'source': 'user',
                            'status': task['status'],
                            'tags': tags,
                        },
                    },
                )
                created.append({'task_key': task['id'], 'db_id': resp['result']})
                print(f"TASK_IMPORTED {task['id']} -> {resp['result']}")

            print('SUMMARY=' + json.dumps({
                'repo_id': repo_id,
                'master_task_id': master_task_id,
                'master_plan_id': master_plan_id,
                'artifact_md_id': artifact_md['result'],
                'artifact_json_id': artifact_json['result'],
                'imported_tasks': created,
                'count': len(created),
            }, ensure_ascii=False))


if __name__ == '__main__':
    anyio.run(main)
