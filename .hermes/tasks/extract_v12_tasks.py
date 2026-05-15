import json
from pathlib import Path

path = Path('/Users/rodrigoortiz/Documents/secoppal/.hermes/tasks/v1.2-plan.json')
data = json.loads(path.read_text())

print('PLAN_TITLE:', data['plan']['title'])
print('PLAN_GOAL:', data['plan']['goal'])
print('TASK_COUNT:', len(data['tasks']))
print('TASKS_JSON_START')
print(json.dumps([
    {
        'id': t['id'],
        'title': t['title'],
        'status': t['status'],
        'hours': t['hours'],
        'week': t['week'],
        'core': t['core'],
        'dependencies': t['dependencies'],
        'done_criteria': t['done_criteria'],
        'risk': t['risk']
    }
    for t in data['tasks']
], ensure_ascii=False, indent=2))
print('TASKS_JSON_END')
