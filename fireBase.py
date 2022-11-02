from flask import Flask, request, jsonify
import json
import requests
import pandas as pd
import random
import collections

application = Flask(__name__, static_url_path='')

baseURL = 'https://dsci-551-c5ba2-default-rtdb.firebaseio.com/project/block'
metaURL = 'https://dsci-551-c5ba2-default-rtdb.firebaseio.com/project/metadata'
lsURL = 'https://dsci-551-c5ba2-default-rtdb.firebaseio.com/project/metadata'

json_suffix = '.json'
block_num = 5


@application.route('/')
def application_init():
    return application.send_static_file("application.html")


@application.route('/operations/mkdir', methods=['GET'])
def mkdir():
    if request.method == "GET":
        user_input = request.args
        directory = user_input.get("dir")
        requests.put(url=lsURL + "/" + directory + json_suffix, json="")
        comb = {
            "command": "mkdir /" + directory
        }
        return jsonify(comb=comb)
    return application.send_static_file("application.html")


@application.route('/operations/ls', methods=['GET'])
def ls():
    if request.method == "GET":
        user_input = request.args
        directory = user_input.get("dir")
        url = lsURL + "/" + directory + json_suffix
        response = requests.get(url=url)
        data = json.loads(response.text)
        res = []
        for key, _ in data.items():
            res.append(key)
        comb = {
            "command": "ls /" + directory,
            "result": res
        }
        return jsonify(comb=comb)
    return application.send_static_file("application.html")


@application.route('/operations/cat', methods=['GET'])
def cat():
    if request.method == "GET":
        user_input = request.args
        file = user_input.get("file")
        file = '/' + file
        index = file.rfind("/")
        fileName = file[index + 1:-4]
        response = requests.get(url=metaURL + "/" + fileName + json_suffix)
        data = json.loads(response.text)
        index = []
        res = []
        for _, val in enumerate(data):
            index.append(val)
        for num in index:
            url = baseURL + str(num) + "/" + fileName + json_suffix
            response = requests.get(url=url)
            data = json.loads(response.text)
            res.extend(data)
        comb = {
            "command": "cat " + file,
            "result": res
        }
        return jsonify(comb=comb)
    return application.send_static_file("application.html")


@application.route('/operations/rm', methods=['GET'])
def rm():
    if request.method == "GET":
        user_input = request.args
        file = user_input.get("file")
        file = '/' + file
        index = file.rfind("/")
        fileName = file[index + 1:-4]
        path = file[:-4]
        response = requests.get(url=metaURL + "/" + fileName + json_suffix)
        data = json.loads(response.text)
        index = []
        for _, val in enumerate(data):
            index.append(val)
        for num in index:
            requests.delete(url=baseURL + str(num) + "/" + fileName + json_suffix)
        requests.delete(url=metaURL + "/" + fileName + json_suffix)
        requests.delete(url=lsURL + path + json_suffix)
        comb = {
            "command": "rm " + file
        }
        return jsonify(comb=comb)
    return application.send_static_file("application.html")


@application.route('/operations/put', methods=['GET'])
def put():
    if request.method == "GET":
        user_input = request.args
        directory = user_input.get("dir")
        file = user_input.get("file")
        partition = int(user_input.get("part"))
        if partition > block_num:
            raise ValueError("partition number invalid: greater than number of total blocks")
        name = '/' + file[:-4]
        dataFrame = pd.read_csv(file)
        data = json.loads(dataFrame.to_json(orient='records', indent=4))
        res = collections.defaultdict(list)
        metainfo = set()
        for item in data:
            index = random.randrange(1, block_num + 1) % partition + 1
            res[index].append(item)
            metainfo.add(index)
        for index, array in res.items():
            requests.put(url=baseURL + str(index) + name + json_suffix, json=array)
        metainfo = {file[:-4]: list(metainfo)}
        requests.patch(url=metaURL + json_suffix, json=metainfo)
        requests.put(url=lsURL + "/" + directory + name + json_suffix, json="")
        comb = {
            "command": "put " + file + " " + directory + " " + str(partition)
        }
        return jsonify(comb=comb)
    return application.send_static_file("application.html")


@application.route('/operations/getloc', methods=['GET'])
def getPartitionLocations():
    if request.method == "GET":
        user_input = request.args
        file = user_input.get("file")
        file = '/' + file
        index = file.rfind("/")
        fileName = file[index + 1:-4]
        response = requests.get(url=metaURL + "/" + fileName + json_suffix)
        data = json.loads(response.text)
        index = []
        for _, val in enumerate(data):
            index.append(val)
        # print(index)
        comb = {
            "command": "getPartitionLocations " + file,
            "result": index
        }
        return jsonify(comb=comb)
    return application.send_static_file("application.html")


@application.route('/operations/readpart', methods=['GET'])
def readPartition():
    if request.method == "GET":
        user_input = request.args
        file = user_input.get("file")
        partition = int(user_input.get("part"))
        file = '/' + file
        index = file.rfind("/")
        fileName = file[index + 1:-4]
        print(baseURL + str(partition) + "/" + fileName + json_suffix)
        response = requests.get(url=baseURL + str(partition) + "/" + fileName + json_suffix)
        data = json.loads(response.text)
        # print(data)
        comb = {
            "command": "readPartition " + file + " " + str(partition),
            "result": data
        }
        return jsonify(comb=comb)
    return application.send_static_file("application.html")


@application.route('/queries', methods=['GET'])
def analytics(file, parameter, type):
    index = getPartitionLocations(file)
    res = []
    for i in index:
        res.append(get_partition_analytics(file, i, parameter, type))
    return analytics_reduce(parameter, res, type)


def get_partition_analytics(file, partition, parameter, type):
    data = readPartition(file, partition)
    res = collections.defaultdict(float)
    if type == 'mean':
        mean = 0.0
        for item in data:
            if parameter in item:
                mean += item[parameter]
        res[mean] = len(data)
    elif type == 'count' or type == 'standard deviation':
        for item in data:
            if parameter in item:
                val = item[parameter]
                res[val] += 1
    elif type == 'min':
        minimum = data[0][parameter]
        for item in data:
            if parameter in item:
                minimum = min(minimum, item[parameter])
        res[parameter] = minimum
    elif type == 'max':
        maximum = data[0][parameter]
        for item in data:
            if parameter in item:
                maximum = max(maximum, item[parameter])
        res[parameter] = maximum
    return res


def analytics_reduce(parameter, list, type):
    res = collections.defaultdict(float)
    if type == 'mean':
        key = 0.0
        val = 0.0
        for item in list:
            for mean, size in item.items():
                key += mean
                val += size
        res[parameter] = float(key / val)
    elif type == 'count':
        for item in list:
            for val, count in item.items():
                res[val] += count
    elif type == 'min':
        minimum = list[0][parameter]
        for item in list:
            minimum = min(minimum, item[parameter])
        res[parameter] = minimum
    elif type == 'max':
        maximum = list[0][parameter]
        for item in list:
            maximum = min(maximum, item[parameter])
        res[parameter] = maximum
    elif type == 'standard deviation':
        import statistics
        array = []
        for item in list:
            for key, val in item.items():
                array.extend([key] * int(val))
        res[parameter] = statistics.stdev(array)
    return res


if __name__ == '__main__':
    application.run()
