"""Private legacy files; only a complete envelope grants their generation authority."""
from contextlib import contextmanager
from contextvars import ContextVar
from pathlib import Path
import os
import shutil

_ACTIVE = ContextVar('continuity_stage', default=None)


def active_generation(store, chat_id):
    active = _ACTIVE.get()
    if active and active[:2] == (str(store.base_path.resolve()), chat_id):
        return active[2]
    return None


def generation_path(store, chat_id, generation, suffix):
    from app.core.conversation_store import _sanitize_chat_id

    if not generation.isalnum():
        raise ValueError('invalid continuity generation')
    return store.base_path / '.generations' / _sanitize_chat_id(chat_id) / generation / suffix


def continuity_path(store, chat_id, suffix, legacy):
    generation = active_generation(store, chat_id)
    if generation is None:
        read = store.load_envelope(chat_id)
        if read.usable:
            generation = read.envelope.continuity_generation
        elif read.status.value == 'CORRUPT':
            # Never resurrect a previous legacy universe after an unreadable publication.
            return store.base_path / '.unavailable' / legacy.name
    return generation_path(store, chat_id, generation, suffix) if generation else legacy


@contextmanager
def stage_continuity(store, reservation):
    if reservation is None:
        yield
        return
    chat = reservation.chat_id
    sources = ((store._path_for(chat), 'history.jsonl'),
               (store._pagination_path_for(chat), 'pagination.json'))
    generation = reservation.result_id
    for source, suffix in sources:
        target = generation_path(store, chat, generation, suffix)
        target.parent.mkdir(parents=True, exist_ok=True)
        if source.exists():
            shutil.copyfile(source, target)
    token = _ACTIVE.set((str(store.base_path.resolve()), chat, generation))
    try:
        yield
    finally:
        _ACTIVE.reset(token)


def prepare_generation(store, reservation):
    generation = active_generation(store, reservation.chat_id)
    if generation:
        directory = generation_path(store, reservation.chat_id, generation, 'history.jsonl').parent
        for path in directory.iterdir():
            if path.is_file():
                with path.open('rb') as handle:
                    os.fsync(handle.fileno())
        # Persist every newly created ancestor before publishing its reference.
        for parent in (directory, *directory.parents):
            descriptor = os.open(parent, os.O_RDONLY)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
            if parent.resolve() == store.base_path.resolve():
                break
    return generation
