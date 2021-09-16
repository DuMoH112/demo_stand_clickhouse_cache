import random

data_generation_scripts_1 = [sorted([random.randint(1000 * j, 10000 * j * i) for j in range(1, i + 1)]) for i in range(1, 15)]

data_generation_scripts_2 = [sorted([random.randint(10000, 200000) for j in range(1, i + 1)]) for i in range(1, 25)]

data_generation_scripts_test = [sorted([random.randint(1000 * j, 10000 * j * i) for j in range(1, i + 1)]) for i in range(1, 2)]
