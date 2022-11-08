from flask import Flask, request, jsonify
import json
import mysql.connector
import os
import numpy as np

application = Flask(__name__, static_url_path='')

config = {
    'user': 'root',
    'password': '123456',
    'host': 'localhost',
    'database': 'dsci551'
}
db = mysql.connector.connect(**config)
cursor = db.cursor()



def init_1():
    return application.send_static_file("application.html")


@application.route('/')
def init():

    cursor.execute("DROP TABLE IF EXISTS metadata")
    cursor.execute("DROP TABLE IF EXISTS structure")
    cursor.execute("DROP TABLE IF EXISTS division")
    cursor.execute("CREATE TABLE metadata (file_id INT UNSIGNED PRIMARY KEY AUTO_INCREMENT, "
                   "name VARCHAR(50), type VARCHAR(10))")
    cursor.execute("CREATE TABLE structure (parent_id INT UNSIGNED, child_id INT UNSIGNED)")
    cursor.execute("CREATE TABLE division (file_id INT UNSIGNED, division INT UNSIGNED, content LONGTEXT)"
                   " ENGINE=InnoDB DEFAULT CHARSET=utf8")

    cursor.execute("INSERT INTO metadata (name, type) VALUES ('{}', '{}')".format('root', 'DIRECTORY'))

    root_id = cursor.lastrowid
    cursor.execute("INSERT INTO structure (parent_id, child_id) VALUES ({}, {})".format('NULL', root_id))
    db.commit()
    return application.send_static_file("application.html")


@application.route('/operations/mkdir', methods=['GET'])
def mkdir():

    if request.method == "GET":
        user_input = request.args
        path = user_input.get("dir")
        path = path.strip()
        directories = path.split('/')

        cursor.execute("SELECT child_id FROM structure WHERE parent_id is NULL")
        parent_id = cursor.fetchone()[0]
        for i in range(len(directories) - 1):
            if directories[i] == '':
                continue
            cursor.execute("SELECT child_id FROM (SELECT file_id FROM metadata WHERE name='{}') as tb1 "
                           "INNER JOIN (SELECT child_id FROM structure WHERE parent_id={}) as tb2 "
                           "ON tb1.file_id=tb2.child_id".format(directories[i], parent_id))
            child_id = cursor.fetchone()
            if child_id is None:
                print("mkdir: cannot create directory ‘{}’: No such file or directory".format(path))
                return
            else:
                child_id = child_id[0]
            parent_id = child_id
        create_directory = directories[len(directories) - 1]
        cursor.execute("INSERT INTO metadata (name, type) VALUES ('{}', '{}')".format(create_directory, 'DIRECTORY'))
        child_id = cursor.lastrowid
        cursor.execute("INSERT INTO structure (parent_id, child_id) VALUES ({}, {})".format(parent_id, child_id))
        db.commit()
        comb = {
            "command": "mkdir /" + path
        }
        return jsonify(comb=comb)
    return application.send_static_file("application.html")


@application.route('/operations/ls', methods=['GET'])
def ls():
    if request.method == "GET":
        user_input = request.args
        path = user_input.get("dir")
        path = path.strip()
        res = []
        directories = path.split('/')
        cursor.execute("SELECT child_id FROM structure WHERE parent_id is NULL")
        parent_id = cursor.fetchone()[0]
        for i in range(len(directories)):
            if directories[i] == '':
                continue
            cursor.execute("SELECT child_id FROM (SELECT file_id FROM metadata WHERE name='{}') as tb1 "
                           "INNER JOIN (SELECT child_id FROM structure WHERE parent_id={}) as tb2 "
                           "ON tb1.file_id=tb2.child_id".format(directories[i], parent_id))
            child_id = cursor.fetchone()
            if child_id is None:
                print("ls: cannot access '{}': No such file or directory".format(path))
                res.append("ls failed: no such directory.")
                comb = {
                    "command": "ls " + path,
                    "result": res
                }
                return jsonify(comb=comb)

            else:
                child_id = child_id[0]
            parent_id = child_id
        cursor.execute("SELECT name FROM metadata "
                       "INNER JOIN (SELECT child_id FROM structure WHERE parent_id={}) as tb1 "
                       "ON metadata.file_id=tb1.child_id".format(parent_id))
        for cur in cursor:
            res.append("/" + cur[0])
            print(cur[0], end="  ")
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
        partitions = int(user_input.get("part"))
        if not os.path.exists(file):
            print("The file {} does not exist".format(file))
            comb = {
                "command": "put failed: no such directory."
            }
            return jsonify(comb=comb)
        path = path.strip()
        directories = path.split('/')
        cursor.execute("SELECT child_id FROM structure WHERE parent_id is NULL")
        parent_id = cursor.fetchone()[0]
        for i in range(len(directories)):
            if directories[i] == '':
                continue
            cursor.execute("SELECT child_id FROM (SELECT file_id FROM metadata WHERE name='{}') as tb1 "
                           "INNER JOIN (SELECT child_id FROM structure WHERE parent_id={}) as tb2 "
                           "ON tb1.file_id=tb2.child_id".format(directories[i], parent_id))
            child_id = cursor.fetchone()
            if child_id is None:
                print("put: cannot access '{}': No such file or directory".format(path))
                return
            else:
                child_id = child_id[0]
            parent_id = child_id
        file_path = file.split('/')
        file_name = file_path[len(file_path) - 1]
        f_size = os.path.getsize(file)
        print(f_size)
        partitions_size = []
        partition_size = int(f_size / partitions)
        print(partition_size)
        for i in range(partitions):
            if i == partitions - 1:
                partitions_size.append(f_size)
            else:
                partitions_size.append(partition_size)
            f_size -= partition_size
        with open(file, encoding='utf-8', mode='r') as f:
            cursor.execute("INSERT INTO metadata (name, type) VALUES ('{}', '{}')".format(file_name, 'FILE'))
            file_id = cursor.lastrowid
            cursor.execute("INSERT INTO structure (parent_id, child_id) VALUES ({}, {})".format(parent_id, file_id))
            for i in range(len(partitions_size)):
                partition = f.read(partitions_size[i])
                sql_insert = 'INSERT INTO division VALUES (%s, %s, %s)'
                cursor.execute(sql_insert, (file_id, i + 1, partition))
        db.commit()
        comb = {
            "command": "put " + file + " " + path + " " + str(partitions)
        }
        return jsonify(comb=comb)
    return application.send_static_file("application.html")


@application.route('/operations/cat', methods=['GET'])
def cat():
    if request.method == "GET":
        user_input = request.args
        path = user_input.get("file")
        path = path.strip()
        res = []
        directories = path.split('/')
        cursor.execute("SELECT child_id FROM structure WHERE parent_id is NULL")
        parent_id = cursor.fetchone()[0]
        for i in range(len(directories)):
            if directories[i] == '':
                continue
            cursor.execute("SELECT child_id FROM (SELECT file_id FROM metadata WHERE name='{}') as tb1 "
                           "INNER JOIN (SELECT child_id FROM structure WHERE parent_id={}) as tb2 "
                           "ON tb1.file_id=tb2.child_id".format(directories[i], parent_id))
            child_id = cursor.fetchone()
            if child_id is None:
                print("cat: cannot access '{}': No such file or directory".format(path))
                return
            else:
                child_id = child_id[0]
            parent_id = child_id
        cursor.execute("SELECT content FROM division WHERE file_id={}".format(parent_id))
        out = ''
        for cur in cursor:
            res.append(cur[0])
            out += cur[0]
        print(out)
        comb = {
            "command": "Command input: cat" + path,
            "result": res
        }
        return jsonify(comb=comb)
    return application.send_static_file("application.html")


@application.route('/operations/getloc', methods=['GET'])
def getPartitionLocations():
    if request.method == "GET":
        user_input = request.args
        path = user_input.get("file")
        path = path.strip()
        res = []
        directories = path.split('/')
        fileName = directories[len(directories) - 1]
        cursor.execute("SELECT child_id FROM structure WHERE parent_id is NULL")
        parent_id = cursor.fetchone()[0]
        for i in range(len(directories)):
            if directories[i] == '':
                continue
            cursor.execute("SELECT child_id FROM (SELECT file_id FROM metadata WHERE name='{}') as tb1 "
                           "INNER JOIN (SELECT child_id FROM structure WHERE parent_id={}) as tb2 "
                           "ON tb1.file_id=tb2.child_id".format(directories[i], parent_id))
            child_id = cursor.fetchone()
            if child_id is None:
                res.append("No such file or directory")
                comb = {
                    "command": "getPartitionLocations " + path,
                    "result": res
                }
                print("mkdir: cannot create directory ‘{}’: No such file or directory".format(path))
                return jsonify(comb=comb)
            else:
                child_id = child_id[0]
            parent_id = child_id
        print(parent_id)
        cursor.execute("SELECT division FROM division WHERE file_id ={}".format(parent_id))

        for i in cursor:
            print(i[0])
            res.append('block:' + str(i[0]) + '--filename:' + fileName)
        comb = {
            "command": "getPartitionLocations " + path,
            "result": res
        }
        return jsonify(comb=comb)
    return application.send_static_file("application.html")


@application.route('/operations/readpart', methods=['GET'])
def readPartition():
    if request.method == "GET":
        user_input = request.args
        path = user_input.get("file")
        partition_number = int(user_input.get("part"))
        path = path.strip()
        res = []
        directories = path.split('/')
        cursor.execute("SELECT child_id FROM structure WHERE parent_id is NULL")
        parent_id = cursor.fetchone()[0]
        for i in range(len(directories)):
            if directories[i] == '':
                continue
            cursor.execute("SELECT child_id FROM (SELECT file_id FROM metadata WHERE name='{}') as tb1 "
                           "INNER JOIN (SELECT child_id FROM structure WHERE parent_id={}) as tb2 "
                           "ON tb1.file_id=tb2.child_id".format(directories[i], parent_id))
            child_id = cursor.fetchone()
            if child_id is None:
                print("readPartition: cannot access '{}': No such file or directory".format(path))
                res.append("No such file or directory")
                comb = {
                    "command": "readPartition " + path + " " + str(partition_number),
                    "result": res
                }
                return jsonify(comb=comb)
            else:
                child_id = child_id[0]
            parent_id = child_id
        cursor.execute("SELECT content FROM division "
                       "WHERE file_id={} and division.division={}".format(parent_id, partition_number))
        for cur in cursor:
            res.append(cur[0])
            print(cur[0])
        comb = {
            "command": "readPartition " + path + " " + str(partition_number),
            "result": res
        }
        return jsonify(comb=comb)
    return application.send_static_file("application.html")


@application.route('/operations/rm', methods=['GET'])
def rm():
    if request.method == "GET":
        user_input = request.args
        path = user_input.get("file")
        path = path.strip()
        res = []
        directories = path.split('/')
        cursor.execute("SELECT child_id FROM structure WHERE parent_id is NULL")
        parent_id = cursor.fetchone()[0]
        for i in range(len(directories)):
            if directories[i] == '':
                continue
            cursor.execute("SELECT child_id FROM (SELECT file_id FROM metadata WHERE name='{}') as tb1 "
                           "INNER JOIN (SELECT child_id FROM structure WHERE parent_id={}) as tb2 "
                           "ON tb1.file_id=tb2.child_id".format(directories[i], parent_id))
            child_id = cursor.fetchone()
            if child_id is None:

                print("rm: cannot access '{}': No such file or directory".format(path))
                res.append("No such file or directory")
                comb = {
                    "command": "rm " + path,
                    "result": res

                }
                return jsonify(comb=comb)
            else:
                child_id = child_id[0]
            parent_id = child_id
        cursor.execute("DELETE FROM metadata WHERE file_id={}".format(parent_id))
        cursor.execute("DELETE FROM structure WHERE child_id={}".format(parent_id))
        cursor.execute("DELETE FROM division WHERE file_id={}".format(parent_id))
        db.commit()
        comb = {
            "command": "rm " + path
        }
        return jsonify(comb=comb)
    return application.send_static_file("application.html")


@application.route('/operations/search', methods=['GET'])
def search():
    if request.method == "GET":
        user_input = request.args
        path = user_input.get("file")
        p = user_input.get("para")
        up_boundary = float(user_input.get("ub"))
        lower_boundary = float(user_input.get("lb"))
        path = path.strip()
        directories = path.split('/')
        cursor.execute("SELECT child_id FROM structure WHERE parent_id is NULL")
        parent_id = cursor.fetchone()[0]
        for i in range(len(directories)):
            if directories[i] == '':
                continue
            cursor.execute("SELECT child_id FROM (SELECT file_id FROM metadata WHERE name='{}') as tb1 "
                           "INNER JOIN (SELECT child_id FROM structure WHERE parent_id={}) as tb2 "
                           "ON tb1.file_id=tb2.child_id".format(directories[i], parent_id))
            child_id = cursor.fetchone()
            if child_id is None:
                print("readPartition: cannot access '{}': No such file or directory".format(path))
                return
            else:
                child_id = child_id[0]
            parent_id = child_id
        cursor.execute("SELECT content FROM division WHERE file_id = {}".format(parent_id))
        content = cursor.fetchone()
        size = len(content)
        data = []
        for i in range(size):
            division_content = content[i]
            all_data = division_content.split('\n')
            for d in all_data:
                data.append(d)
        columns = data[0].split(',')
        match = dict()
        for i in range(len(columns)):
            match[columns[i]] = i
        res = []
        columns_id = match[p]
        for i in range(len(data)):
            if i != 0:
                row_data = data[i].split(',')
                temp_ans = float(row_data[columns_id])
                if lower_boundary <= temp_ans <= up_boundary:
                    res.append(row_data)
        json_data = json.dumps(res, ensure_ascii=False)
        return json_data
    return application.send_static_file("application.html")


@application.route('/operations/analytics', methods=['GET'])
def analytics():
    if request.method == "GET":
        user_input = request.args
        file = user_input.get("file")
        p = user_input.get("para")
        mode = user_input.get("type")
        res = map_partition(file, p)
        ans = reduce_partition(res, mode)
        return ans
    return application.send_static_file("application.html")


def map_partition(path, p):
    path = path.strip()
    directories = path.split('/')
    cursor.execute("SELECT child_id FROM structure WHERE parent_id is NULL")
    parent_id = cursor.fetchone()[0]
    for i in range(len(directories)):
        if directories[i] == '':
            continue
        cursor.execute("SELECT child_id FROM (SELECT file_id FROM metadata WHERE name='{}') as tb1 "
                       "INNER JOIN (SELECT child_id FROM structure WHERE parent_id={}) as tb2 "
                       "ON tb1.file_id=tb2.child_id".format(directories[i], parent_id))
        child_id = cursor.fetchone()
        if child_id is None:
            print("readPartition: cannot access '{}': No such file or directory".format(path))
            return
        else:
            child_id = child_id[0]
        parent_id = child_id
    cursor.execute("SELECT content FROM division WHERE file_id = {}".format(parent_id))
    content = cursor.fetchone()
    size = len(content)
    data = []
    for i in range(size):
        division_content = content[i]
        all_data = division_content.split('\n')
        for d in all_data:
            data.append(d)
    columns = data[0].split(',')
    match = dict()
    for i in range(len(columns)):
        match[columns[i]] = i
    res = []
    columns_id = match[p]
    for i in range(len(data)):
        if i != 0:
            row_data = data[i].split(',')
            res.append(float(row_data[columns_id]))
    return res


def reduce_partition(res, mode):
    if mode == 'mode':
        counts = np.bincount(res)
        return json.dumps(str(np.argmax(counts)))
    if mode == 'median':
        return json.dumps(str(np.median(res)))
    if mode == 'mean':
        return json.dumps(str(np.mean(res)))
    if mode == 'max':
        return json.dumps(str(np.max(res)))
    if mode == 'min':
        return json.dumps(str(np.min(res)))
    if mode == 'std':
        return json.dumps(str(np.std(res)))
    if mode == 'var':
        return json.dumps(str(np.var(res)))


if __name__ == '__main__':
    application.run()