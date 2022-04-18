from utils.iterators import QueueIterator


class Queue:
    
    def __init__(self) -> None:
        self.__data = list()


    def enqueue(self, item):
        self.__data.append(item)
    
    def poll(self):
        if self.size() == 0:
            raise IndexError('Queue is empty!! Populate first before polling.')
        return self.__data.pop(0)

    def is_empty(self):
        return len(self.__data) == 0

    def size(self):
        return len(self.__data)

    def peek(self):
        return self.__data[0]

    def gen_iter(self):
        return iter(QueueIterator(self))