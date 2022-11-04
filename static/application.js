window.onload = async function() {
    document.getElementById("form").reset();
    await fetchAndPrint(lsurl + base, 2);

}

let base = '/';
let lsurl = "/operations/ls?dir=";

async function fetchAndPrint(url, source){
    let response = await fetch(url);
    let json = await response.json();
    let command = json.comb.command;
    let result = json.comb.result;

    if(source === 1){
        document.getElementById("commandA").innerHTML = "$ " + command;
        document.getElementById("resultA").innerHTML = JSON.stringify(result, null, 2);
    }

    if (source == 0 || source == 3){
        document.getElementById(source == 0 ? "command" : "commandS").innerHTML = "$ " + command;
        if (result != null) {
            if (document.getElementById("op").value === "cat" || document.getElementById("op").value === "readPartition") {
                document.getElementById("result").innerHTML = JSON.stringify(result, null, 2);
            }
            else {
                let out = "";
                for (let i = 0; i < result.length; i++) {
                    out += result[i] + "\n";
                }
                document.getElementById(source == 0 ? "result" : "resultS").innerHTML = out;
            }
        }
    } else {
        let out = "";
        for (let i = 0; i < result.length; i++) {
            out += "<a href ='#' onclick='navigateDir(\"" + result[i] + "\")'>" + result[i] + "</a>&nbsp&nbsp&nbsp|&nbsp&nbsp&nbsp";
        }
        out += "&nbsp&nbsp&nbsp---&nbsp&nbsp&nbsp<a href ='#' onclick='navigateBack()'>Back</a>";
        console.log(out)
        document.getElementById("dir_map").innerHTML = out;
    }
}
async function navigateDir(path) {
    base += path;
    await fetchAndPrint(lsurl + base, 2);
}

async function navigateBack() {
    base = base.substring(0, base.lastIndexOf('/'));
    base += base.length == 0 ? "/" : "";
    await fetchAndPrint(lsurl + base, 2);
}

async function ShowSearchResult(){
    let range = [null, null];
    if (document.getElementById("eqVal").value) {
        range[0] = document.getElementById("eqVal").value;
    } else {
        range[0] = document.getElementById("lbVal").value;
        range[1] = document.getElementById("ubVal").value;
    }
    await fetchAndPrint("/operations/search?" + "file=" + document.getElementById("filepathS").value
        + "&para=" + document.getElementById("parameterS").value + "&lb=" + range[0] + "&ub=" + range[1], 3);
}


function ShowAnalyticsResult(){
    file = document.getElementById("filepathA").value;
    para = document.getElementById("parameter").value;
    type = document.getElementById("opA").value;
    url = "/operations/analytics?" + "file=" + file + "&para=" + para + "&type=" + type;
    fetchAndPrint(url, 1)
}

function ShowResult() {
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
    fetchAndPrint(url, 0)
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
    } else if (document.getElementById("op").value === "ls") {
        document.getElementById("filelabel").style.display = "none";
        document.getElementById("direclabel").style.display = "flex";
        document.getElementById("partlabel").style.display = "none";
        document.getElementById("filepath").style.display = "none";
        document.getElementById("directory").style.display = "flex";
        document.getElementById("partition").style.display = "none";
    } else if (document.getElementById("op").value === "cat") {
        document.getElementById("filelabel").style.display = "flex";
        document.getElementById("direclabel").style.display = "none";
        document.getElementById("partlabel").style.display = "none";
        document.getElementById("filepath").style.display = "flex";
        document.getElementById("directory").style.display = "none";
        document.getElementById("partition").style.display = "none";
    } else if (document.getElementById("op").value === "rm") {
        document.getElementById("filelabel").style.display = "flex";
        document.getElementById("direclabel").style.display = "none";
        document.getElementById("partlabel").style.display = "none";
        document.getElementById("filepath").style.display = "flex";
        document.getElementById("directory").style.display = "none";
        document.getElementById("partition").style.display = "none";
    } else if (document.getElementById("op").value === "put") {
        document.getElementById("filelabel").style.display = "flex";
        document.getElementById("direclabel").style.display = "flex";
        document.getElementById("partlabel").style.display = "flex";
        document.getElementById("filepath").style.display = "flex";
        document.getElementById("directory").style.display = "flex";
        document.getElementById("partition").style.display = "flex";
    } else if (document.getElementById("op").value === "getPartitionLocations") {
        document.getElementById("filelabel").style.display = "flex";
        document.getElementById("direclabel").style.display = "none";
        document.getElementById("partlabel").style.display = "none";
        document.getElementById("filepath").style.display = "flex";
        document.getElementById("directory").style.display = "none";
        document.getElementById("partition").style.display = "none";
    } else if (document.getElementById("op").value === "readPartition") {
        document.getElementById("filelabel").style.display = "flex";
        document.getElementById("direclabel").style.display = "none";
        document.getElementById("partlabel").style.display = "flex";
        document.getElementById("filepath").style.display = "flex";
        document.getElementById("directory").style.display = "none";
        document.getElementById("partition").style.display = "flex";
    }
}

function GetSOperation() {
    let op = document.getElementById("opS").value;
    document.getElementById("eqVal").value = null;
    document.getElementById("lbVal").value = null;
    document.getElementById("ubVal").value = null;
    document.getElementById("eqIn").style.display = op == "Equality" ? "flex" : "none";
    document.getElementById("rangeIn").style.display = op == "Equality" ? "none" : "flex";
}
