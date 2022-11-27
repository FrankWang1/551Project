import json
from pymongo import MongoClient
import pandas as pd
import numpy as np
from collections import Counter
import statistics
from flask import Flask, request, jsonify

application = Flask(__name__, static_url_path='')

BLOCKS, client = 6, MongoClient('mongodb://localhost:27017/')
name_node, data_nodes = client['edfs'], client['edfs_data_nodes']


@application.route('/')
def application_init():
    return application.send_static_file("application.html")


@application.route('/operations/mkdir', methods=['GET'])
def mkdir():
    if request.method == "GET":
        user_input = request.args
        path = user_input.get("dir")
        collection = name_node[path]
        if collection.find_one():
            print('mkdir failed: directory already exists.')
            return
        dirs = path.split('/')
        cur = '/'
        for i in range(1, len(dirs) - 1):
            cur = ''.join([cur, dirs[i]])
            name_node[cur].insert_one({'dir': dirs[i + 1]})
            cur = ''.join([cur, '/'])
        collection.insert_one({'dir': None})
        comb = {
            "command": "mkdir " + path
        }
        return jsonify(comb=comb)
    return application.send_static_file("application.html")


@application.route('/operations/ls', methods=['GET'])
def ls():
    if request.method == "GET":
        user_input = request.args
        path = user_input.get("dir")
        res = []
        if path == '/':
            for name in name_node.list_collection_names():
                res.append(name[1:])
            print(res)
            comb = {"command": "ls " + path,"result": res}
            return jsonify(comb=comb)
        if not name_node[path].find_one():
            print('ls failed: no such directory.')
            res.append("ls failed: no such directory.")
            comb = {
                "command": "ls " + path,
                "result": res
            }
            return jsonify(comb=comb)
        for doc in name_node[path].find():
            if 'file' in doc:
                print(doc['file'])
                res.append(doc['file'])
            if 'dir' in doc and doc['dir']:
                print('(dir)', doc['dir'])
                res.append("/" + doc['dir'])
        comb = {
            "command": "ls " + path,
            "result": res
        }
        return jsonify(comb=comb)
    return application.send_static_file("application.html")


@application.route('/operations/put', methods=['GET'])
def put():
    if request.method == "GET":
        user_input = request.args
        file = user_input.get("file")
        path = user_input.get("dir")
        k = int(user_input.get("part"))
        print(file)
        print(path)
        print(k)
        if not name_node[path].find_one():
            print('put failed: no such directory.')
            comb = {
                "command": "put failed: no such directory."
            }
            return jsonify(comb=comb)
        # if name_node[path].find_one({'file': file}):
        #     print(name_node[path].find_one({'file': file}))
        #     print('put failed: file already exists.')
        #     comb = {
        #         "command": "put failed: file already exists."
        #     }
        #     return jsonify(comb=comb)
        df = pd.read_csv(file)
        split_list = np.array_split(range(len(df.index)), k)
        locs = []
        for i in range(k):
            file_name = ''.join([file, str(i)])
            locs.append({'block': i % BLOCKS, 'file': file_name})
            act_data = []
            for j in split_list[i]:
                act_data.append(df.loc[j].to_json())
            data_nodes[''.join(['block', str(i % BLOCKS)])].insert_one(
                {'file': file_name, 'data': act_data})
        name_node[path].insert_one({'file': file, 'locs': locs})
        comb = {
            "command": "put " + file + " " + path + " " + str(k)
        }
        return jsonify(comb=comb)
    return application.send_static_file("application.html")


def splitFilename(path):
    if path[-1] == '/':
        print('failed loading: no such file.')
        return None
    split = path.rindex('/')
    dir, filename = path[:split], path[split + 1:]
    file = name_node[dir].find_one({'file': filename})
    if not file:
        print('failed loading: no such file.')
        return None
    return file


@application.route('/operations/cat', methods=['GET'])
def cat():
    if request.method == "GET":
        user_input = request.args
        path = user_input.get("file")
        file = splitFilename(path)
        res = []
        if file:
            for loc in file['locs']:
                for line in data_nodes[''.join(['block', str(loc['block'])])].find_one({'file': loc['file']})['data']:
                    print(type(line))
                    res.append(json.loads(line))
        comb = {
            "command": "Command input: cat" + path,
            "result": res
        }
        return jsonify(comb=comb)
    return application.send_static_file("application.html")


@application.route('/operations/rm', methods=['GET'])
def rm():
    if request.method == "GET":
        user_input = request.args
        path = user_input.get("file")
        file = splitFilename(path)
        if file:
            for loc in file['locs']:
                data_nodes[''.join(['block', str(loc['block'])])].delete_one({'file': loc['file']})
        split = path.rindex('/')
        name_node[path[:split]].delete_one({'file': path[split + 1:]})
        comb = {
            "command": "rm " + path
        }
        return jsonify(comb=comb)
    return application.send_static_file("application.html")


@application.route('/operations/getloc', methods=['GET'])
def getPartitionLocations():
    if request.method == "GET":
        user_input = request.args
        path = user_input.get("file")
        file = splitFilename(path)
        count = 0
        res = []
        if file:
            for loc in file['locs']:
                # print('block:', loc['block'], '-- filename:', loc['file'])
                res.append('block:' + str(loc['block']) + '-- filename:' + str(loc['file']))
                count = count + 1
        comb = {
            "command": "getPartitionLocations " + path,
            "result": res
        }
        return jsonify(comb=comb)
    return application.send_static_file("application.html")


def getPartitionLocations(path):
        file = splitFilename(path)
        count = 0
        if file:
            for loc in file['locs']:
                # print('block:', loc['block'], '-- filename:', loc['file'])
                count = count + 1
        return count


@application.route('/operations/readpart', methods=['GET'])
def readPartition():
    if request.method == "GET":
        user_input = request.args
        path = user_input.get("file")
        k = int(user_input.get("part"))
        file = splitFilename(path)
        array = []
        if file:
            for line in \
                    data_nodes[''.join(['block', str(file['locs'][k]['block'])])].find_one(
                        {'file': file['locs'][k]['file']})['data']:
                array.append(json.loads(line))
        comb = {
            "command": "readPartition " + path + " " + str(k),
            "result": array
        }
        return jsonify(comb=comb)
    return application.send_static_file("application.html")


def readPartition(path, k):
    file = splitFilename(path)
    array = []
    if file:
        for line in \
                data_nodes[''.join(['block', str(file['locs'][k]['block'])])].find_one(
                    {'file': file['locs'][k]['file']})['data']:
            array.append(json.loads(line))
    return array


def reduce(aType, array):
    if aType == 'mean':
        sum_array = 0
        sum_size = 0
        for sub_array in array:
            sum_array += sub_array[0] * sub_array[1]
            sum_size += sub_array[1]
        mean = sum_array/sum_size
        return mean

    if aType == 'SD':
        sd_array = []
        for sub_array in array:
            sd_array += sub_array
        return statistics.pstdev(sd_array)

    if aType == 'max':
        max_num = max(array)
        return max_num

    if aType == 'min':
        min_num = min(array)
        return min_num

    if aType == 'count':
        count_array = []
        for sub_array in array:
            count_array = sub_array + count_array
        count_dict = Counter(count_array)
        return count_dict


def analyticsByPartition(file, parameter, aType, partition):
    array = readPartition(file, partition)
    if aType == 'mean':
        count = 0
        sum = 0
        for i in array:
            sum += int(i.get(parameter))
            count += 1
        return [sum/count, count]

    if aType == 'SD':
        sd_array = []

        for i in array:
            sd_array.append(int(i.get(parameter)))
        return sd_array

    if aType == 'max':
        max_array = []
        for i in array:
            max_array.append(int(i.get(parameter)))
        return max(max_array)

    if aType == 'min':
        min_array = []
        for i in array:
            min_array.append(int(i.get(parameter)))
        return min(min_array)

    if aType == 'count':
        count_array = []
        for i in array:
            count_array.append(i.get(parameter))
        return count_array


@application.route('/operations/search', methods=['GET'])
def search():
    path, field, lb, ub = request.args.get("file"), request.args.get("para"), request.args.get("lb"), request.args.get("ub")
    ub = None if ub == "null" else ub
    file = splitFilename(path)
    res, result = [], []
    if not file:
        comb = {"command": "Failed: file not exists.", "result": res}
        return jsonify(comb=comb)
    i = 0
    for loc in file['locs']:
        pres = []
        for line in data_nodes[''.join(['block', str(loc['block'])])].find_one({'file': loc['file']})['data']:
            val = json.loads(line)[field]
            lb = int(lb) if type(val) == int else (float(lb) if type(val) == float else lb)
            ub = None if not ub else (int(ub) if type(val) == int else (float(ub) if type(val) == float else ub))
            if ub and val >= lb and val <= ub or not ub and val == lb:
                res.append(json.loads(line))
                pres.append(json.loads(line))
        result.append({"partition " + str(i): pres})
        i += 1
    result.append({"total": res})
    comb = {"command": "Records found:", "result": result}
    print(comb)
    return jsonify(comb=comb)

@application.route('/operations/analytics', methods=['GET'])
def analytics():
    if request.method == "GET":
        user_input = request.args
        file = user_input.get("file")
        parameter = user_input.get("para")
        aType = user_input.get("type")
        count = getPartitionLocations(file)
        array = []
        res = []
        res.append("Map part:")
        for i in range(count):
            # print("partition" + str(i))
            num = analyticsByPartition(file, parameter, aType, i)
            array.append(num)
            res.append({"Partition " + str(i): num})
        result = reduce(aType, array)
        res.append("Reduce part:")
        res.append({"Reduce:": str(result)})
        comb = {
            "command": "Analytics: In the file [" + file + "], we want to get [" + aType + "] of [" + parameter + "]. "
                       + "And We have two parts: Map and Reduce",
            "result": res
        }
        print(comb)
        return jsonify(comb=comb)
    return application.send_static_file("application.html")


if __name__ == '__main__':
    application.run()

# # mkdir('/user')
# # put('Case.csv', '/user', 17)
# #
# # cat('/user/Case.csv')
# # rm('/user/Case.csv')
# # put('Case.csv', '/user', 17)
# # cat('/user/Case.csv')
# # ls('/user')
#
# # mkdir('/user/kevin')
# # ls('/user')
# # getPartitionLocations('/user/Case.csv')
# readPartition('/user/Case.csv', 7)
#
# # mean = analytics("/user/Case.csv", 'confirmed', 'mean')
# # print("final answer(mean): " + str(mean))
# #
# # max_num = analytics("/user/Case.csv", 'confirmed', 'max')
# # print("final answer(max): " + str(max_num))
# #
# # min_num = analytics("/user/Case.csv", 'confirmed', 'min')
# # print("final answer(): " + str(min_num))
#
# # count = analytics("/user/Case.csv", 'province', 'count')
# # print("final answer(count): " + str(count))
#
# # sd = analytics("/user/Case.csv", 'confirmed', 'SD')
# # print("final answer(SD): " + str(sd))