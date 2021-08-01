import math
import random
import matplotlib.pyplot as plt
from os import popen
from shutil import rmtree
import glob


def main():
    lower, upper = 0, 10
    points = []
    with open("input/points.txt", "w") as f:
        for _ in range(1_000):
            x = random.random() * (upper - lower) + lower
            y = random.random() * (upper - lower) + lower
            f.write(f"{x:.2f},{y:.2f}\n")
            points.append([x, y])

    rmtree("output")
    stream = popen('spark-submit --master local[*] --class KMean k-mean.jar')
    output = stream.read()
    print(output)

    read_files = glob.glob("output/part*")

    with open("output/centroids.txt", "wb") as outfile:
        for f in read_files:
            with open(f, "rb") as infile:
                outfile.write(infile.read())

    centroids = []
    colors = dict()

    with open("output/centroids.txt", "r") as f:
        for line in f.readlines():
            tokens = line.lstrip("(").rstrip(")").split(",")
            point = (float(tokens[1]), float(tokens[2]))
            centroids.append(point)
            colors[point] = random_color()

    for point in points:
        plt.scatter(point[0], point[1], c=get_color(point, colors))
    plt.show()


def get_color(point, colors):
    min_distance = -1
    centroid = point
    for key in colors:
        x_diff = math.fabs(key[0] - point[0])
        y_diff = math.fabs(key[1] - point[1])
        distance = math.sqrt(x_diff ** 2 + y_diff ** 2)
        if min_distance == -1 or min_distance > distance:
            min_distance = distance
            centroid = key
    return colors[centroid]


def random_color():
    return "#" + ''.join([random.choice('0123456789ABCDEF') for _ in range(6)])


if __name__ == '__main__':
    main()
