import time
import zmq
import sys
import os
import random

WORKERS = 2

def merge_sort(part1, part2):
    result = []
    i1 = 0
    i2 = 0
    while len(result) < len(part1) + len(part2):
        if i1 < len(part1):
            n1 = part1[i1]
        else:
            n1 = None
        if i2 < len(part2):
            n2 = part2[i2]
        else:
            n2 = None
        if (n1 is not None) and (n2 is None or n1 <= n2):
            result.append(n1)
            i1 += 1
        else:
            result.append(n2)
            i2 += 1
    return result

def spawn_worker():
    if os.fork() == 0:
        context = zmq.Context()

        pull = context.socket(zmq.PULL)
        pull.connect("ipc:///tmp/push")
        killer = context.socket(zmq.SUB)
        killer.connect("ipc:///tmp/killer")
        killer.setsockopt(zmq.SUBSCRIBE, "")
        worker_result = context.socket(zmq.PUSH)
        worker_result.connect("ipc:///tmp/worker_result")

        poller = zmq.Poller()
        poller.register(pull, zmq.POLLIN)
        poller.register(killer, zmq.POLLIN)
        while True:
            socks = dict(poller.poll())
            if pull in socks and socks[pull] == zmq.POLLIN:
                part1, part2 = pull.recv_pyobj()
                worker_result.send_pyobj(merge_sort(part1, part2))
            if killer in socks and socks[killer] == zmq.POLLIN:
                print "KILL"
                sys.exit(0)

def main():
    context = zmq.Context()

    push = context.socket(zmq.PUSH)
    push.bind("ipc:///tmp/push")
    killer = context.socket(zmq.PUB)
    killer.bind("ipc:///tmp/killer")
    worker_result = context.socket(zmq.PULL)
    worker_result.bind("ipc:///tmp/worker_result")

    for _ in range(WORKERS):
        spawn_worker()

    gen = [random.randint(0, 1000000) for _ in xrange(100000)].__iter__()

    poller = zmq.Poller()
    poller.register(worker_result, zmq.POLLIN)
    poller.register(push, zmq.POLLOUT)
    results = []
    while True:
        socks = dict(poller.poll())
        if worker_result in socks and socks[worker_result] == zmq.POLLIN:
            result = worker_result.recv_pyobj()
            if len(result) == 100000:
                killer.send_pyobj("KILL")
                assert result == sorted(result)
                break
            else:
                results.append(result)
                if len(results) == 2:
                    push.send_pyobj(results)
                    results = []
        if push in socks and socks[push] == zmq.POLLOUT:
            try:
                push.send_pyobj(([gen.next()], [gen.next()]))
            except StopIteration:
                poller.unregister(push)

if __name__ == "__main__":
    main()
