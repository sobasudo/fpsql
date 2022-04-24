class QueueIterator(object):
    def __init__(self, queue) -> None:
        self.queue = queue

    def __iter__(self):
        while not self.queue.is_empty():
            yield self.queue.poll()
