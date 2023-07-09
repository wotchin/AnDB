import threading


def spinlock_create():
    return threading.Lock()


def spinlock_aquire(lock):
    while True:
        acquired = lock.acquire(timeout=0.1)
        if acquired:
            break


def spinlock_release(lock):
    lock.release()
