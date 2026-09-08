"""Is 'barrier then reset' promoting an unpublished turn PREEXISTING?"""
import os, sys, tempfile
sys.path.insert(0, '.')
from app.core.conversation_store import ConversationStore
from app.core.workflow.publication_io import barrier_path
from app.core.workflow.store_epoch import advance_epoch

base = tempfile.mkdtemp()
store = ConversationStore(base)
chat = 'probe'


def publish(query):
    r = store.reserve_turn(chat)
    return store.publish_turn(r, user_query=query, response='r', profile_state=None, continuation=None)


publish('A confirmado')
path = store._envelope_path_for(chat)
original, reached = os.fsync, []
def fsync(fd):
    if path.exists() and not reached and os.fstat(fd).st_ino == path.stat().st_ino:
        reached.append('x'); raise OSError('fail')
    return original(fd)
os.fsync = fsync
try:
    publish('B NO publicado')
except OSError:
    pass
os.fsync = original
print('barrier present:', barrier_path(path).exists())  # noqa: T201
print('before reset usable:', store.load_envelope(chat).usable)  # noqa: T201
advance_epoch(path)
read = store.load_envelope(chat)
print('after reset usable:', read.usable, '| query:', read.envelope.user_query if read.usable else None)  # noqa: T201
