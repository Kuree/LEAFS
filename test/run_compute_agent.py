try:
    from agent import ComputeAgent
except:
    import os, sys
    sys.path.insert(1, os.path.join(sys.path[0], '..'))
    from agent import ComputeAgent

if __name__ == "__main__":
    a = ComputeAgent(block_current_thread=True, is_benchmark=True)
    a.connect()