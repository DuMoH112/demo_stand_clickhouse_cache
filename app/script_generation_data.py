import random

data_generation_scripts_big_different = [sorted([random.randint(1000 * j, 10000 * j * i) for j in range(1, i + 1)]) for i in range(1, 19)]

data_generation_scripts_low_different = [sorted([random.randint(10000, 200000) for j in range(1, i + 1)]) for i in range(1, 30)]

data_generation_scripts_test = [sorted([random.randint(1000 * j, 10000 * j * i) for j in range(1, i + 1)]) for i in range(1, 2)]
