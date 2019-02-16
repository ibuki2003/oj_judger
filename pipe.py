import os
class pipe:
    def __init__(self):
        self.r, self.w = os.pipe()

    def __enter__(self):
        return self

    def __exit__(self, ex_type, ex_value, trace):
        os.close(self.r)
        os.close(self.w)
