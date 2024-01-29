from queue import Queue
from threading import Thread

class Worker(Thread):
    def __init__(self, tasks):
        Thread.__init__(self)
        self.tasks = tasks
        self.daemon = True
        self.start()

    def run(self):
        while True:
            task = self.tasks.get()
            if task is None:  # Verifica se é o token de parada
                break
            func, args, kargs = task
            try:
                func(*args, **kargs)
            except:
                pass
            finally:
                self.tasks.task_done()

class ThreadPool:
    def __init__(self, num_threads):
        self.tasks = Queue(num_threads)
        self.workers = [Worker(self.tasks) for _ in range(num_threads)]

    def add_task(self, func, *args, **kargs):
        self.tasks.put((func, args, kargs))

    def wait_completion(self):
        self.tasks.join()

    def close(self):
        # Coloca um token de parada na fila para cada thread
        for _ in self.workers:
            self.tasks.put(None)
        # Espera todas as threads terminarem
        for worker in self.workers:
            worker.join()
