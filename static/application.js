window.onload = function() {
    document.getElementById("form").reset();
}

async function ShowResult() {
    let url = "";
    let file = "";
    let dir = ""
    let part = "";
    if (document.getElementById("op").value === "mkdir") {
        dir = document.getElementById("directory").value;
        url = "/operations/mkdir?" + "dir=" + dir;
    }
    else if (document.getElementById("op").value === "ls") {
        dir = document.getElementById("directory").value;
        url = "/operations/ls?" + "dir=" + dir;
    }
    else if (document.getElementById("op").value === "cat") {
        file = document.getElementById("filepath").value;
        url = "/operations/cat?" + "file=" + file;
    }
    else if (document.getElementById("op").value === "rm") {
        file = document.getElementById("filepath").value;
        url = "/operations/rm?" + "file=" + file;
    }
    else if (document.getElementById("op").value === "put") {
        dir = document.getElementById("directory").value;
        file = document.getElementById("filepath").value;
        part = document.getElementById("partition").value;
        url = "/operations/put?" + "file=" + file + "&dir=" + dir + "&part=" + part;
    }
    else if (document.getElementById("op").value === "getPartitionLocations") {
        file = document.getElementById("filepath").value;
        url = "/operations/getloc?" + "file=" + file;
    }
    else if (document.getElementById("op").value === "readPartition") {
        file = document.getElementById("filepath").value;
        part = document.getElementById("partition").value;
        url = "/operations/readpart?" + "file=" + file + "&part=" + part;
    }
    let response = await fetch(url);
    let json = await response.json();
    let command = json.comb.command;
    let result = json.comb.result;
    document.getElementById("command").innerHTML = "$ " + command;
    if (result != null) {
        if (document.getElementById("op").value === "cat" || document.getElementById("op").value === "readPartition") {
            document.getElementById("result").innerHTML = JSON.stringify(result, null, 2);
        }
        else {
            let out = "";
            for (let i = 0; i < result.length; i++) {
                out += result[i] + " ";
            }
            document.getElementById("result").innerHTML = out;
        }
    }
}

function GetOperation() {
    let op = document.getElementById("op").value;
    document.getElementById("form").reset();
    document.getElementById("op").value = op;
    if (document.getElementById("op").value === "mkdir") {
        document.getElementById("filelabel").style.display = "none";
        document.getElementById("direclabel").style.display = "flex";
        document.getElementById("partlabel").style.display = "none";
        document.getElementById("filepath").style.display = "none";
        document.getElementById("directory").style.display = "flex";
        document.getElementById("partition").style.display = "none";
    }
    else if (document.getElementById("op").value === "ls") {
        document.getElementById("filelabel").style.display = "none";
        document.getElementById("direclabel").style.display = "flex";
        document.getElementById("partlabel").style.display = "none";
        document.getElementById("filepath").style.display = "none";
        document.getElementById("directory").style.display = "flex";
        document.getElementById("partition").style.display = "none";
    }
    else if (document.getElementById("op").value === "cat") {
        document.getElementById("filelabel").style.display = "flex";
        document.getElementById("direclabel").style.display = "none";
        document.getElementById("partlabel").style.display = "none";
        document.getElementById("filepath").style.display = "flex";
        document.getElementById("directory").style.display = "none";
        document.getElementById("partition").style.display = "none";
    }
    else if (document.getElementById("op").value === "rm") {
        document.getElementById("filelabel").style.display = "flex";
        document.getElementById("direclabel").style.display = "none";
        document.getElementById("partlabel").style.display = "none";
        document.getElementById("filepath").style.display = "flex";
        document.getElementById("directory").style.display = "none";
        document.getElementById("partition").style.display = "none";
    }
    else if (document.getElementById("op").value === "put") {
        document.getElementById("filelabel").style.display = "flex";
        document.getElementById("direclabel").style.display = "flex";
        document.getElementById("partlabel").style.display = "flex";
        document.getElementById("filepath").style.display = "flex";
        document.getElementById("directory").style.display = "flex";
        document.getElementById("partition").style.display = "flex";
    }
    else if (document.getElementById("op").value === "getPartitionLocations") {
        document.getElementById("filelabel").style.display = "flex";
        document.getElementById("direclabel").style.display = "none";
        document.getElementById("partlabel").style.display = "none";
        document.getElementById("filepath").style.display = "flex";
        document.getElementById("directory").style.display = "none";
        document.getElementById("partition").style.display = "none";
    }
    else if (document.getElementById("op").value === "readPartition") {
        document.getElementById("filelabel").style.display = "flex";
        document.getElementById("direclabel").style.display = "none";
        document.getElementById("partlabel").style.display = "flex";
        document.getElementById("filepath").style.display = "flex";
        document.getElementById("directory").style.display = "none";
        document.getElementById("partition").style.display = "flex";
    }
}
