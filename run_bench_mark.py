from test.benchmark_stream import benchmark_stream
from multiprocessing.pool import ThreadPool
import time


if __name__ == "__main__":
    thread_size = 5
    result = []
    def test_one_thread(count):
        b = benchmark_stream(count)
        pool = ThreadPool(processes=4)
        apply_result = pool.apply_async(b.run)
        send_time, receive_time = apply_result.get()
        result.append((send_time, receive_time))
        count += 1

    
    for i in range(thread_size):
        test_one_thread(i)

    def shouldStop():
        global result, thread_size
        lst = [x for x in result if x is not None]
        if len(lst) == thread_size:
            return True
        return False

    while True:
        if not shouldStop():
            time.sleep(10)
        else:
            break

    print(result)

