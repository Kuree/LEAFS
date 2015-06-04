from agent import ComputeAgent

if __name__ == "__main__":
    a = ComputeAgent(block_current_thread=True, is_benchmark=True)
    a.connect()