/**
 * Created with IntelliJ IDEA.
 * User: yxn
 * Date: 14-1-23
 * Time: 下午2:46
 * To change this template use File | Settings | File Templates.
 */
var slaPageViewDate = getformattedDate();
$(function () {
    /**
     * 初始化table和partition
     * */
    var $datepicker = $('#run-date').val(getformattedDate());
    $datepicker.datepicker({
        format: 'yyyy-mm-dd'
    });
    getSlaData();
    $("#refresh-button").click(function(){
        getSlaData();
    })
    $("#his-view").click(function(){
        if(!$("#fail-cotent").is(":hidden")){
            $("#fail-cotent").hide();
        }
        slaPageViewDate = $('#run-date').val();
        if(slaPageViewDate!=getformattedDate()){
            $("#title").html('');
            $("#title").append(slaPageViewDate+" SLA任务完成情况");
            $("#result-body").html('');
            $("#sla_static").html('');
        }
        else{
            $("#title").html('');
            $("#title").append("SLA任务实时状态列表");
            $("#result-body").html('');
            $("#sla_static").html('');
        }
        getSlaData();
    })
    $("#close-button").click(function(){
        createReport();
    })

});

function statusMap(status){
    switch (status){
        case 0:
            return "init";
        case -1:
            return "fail";
        case 1:
            return "success";
        case 0:
            return "init";
        case 2:
            return "running";
        case 3:
            return "suspend";
        case 4:
            return "init error";
        case 5:
            return "wait";
        case 6:
            return "ready";
        case 7:
            return "timeout";
        default:
            return status+" ";
    }
}

function createRow(slaObj) {
    var addClass = "";
    var statdesc = "";
    var status = slaObj.reportStatus;
    switch(status){
        case 0:
            addClass = "info";
            statdesc = "运行中";
            break;
        case 1:
            addClass = "success";
            statdesc = "按时完成";
            break;
        case 2:
            addClass = "warning";
            statdesc = "已完成(延迟)";
            break;
        case 3:
            addClass = "info";
            statdesc = "运行中";
            break;
        case -1:
            addClass = "error";
            statdesc = "预计延迟";
            break;
    }
    var type = slaObj.slaType;
    var typedesc = "";
    switch (type){
        case 0:
            typedesc = "自定义"
            break;
        case 1:
            typedesc = "北斗报表"
            break;
        case 2:
            typedesc = "邮件任务"
            break;
        case 3:
            typedesc = "dashboard"
            break;
        case 4:
            typedesc = "线上应用"
            break;
    }
    if(slaObj.keyPreStatus == null){
        slaObj.keyPreStatus = 'NA'
    }
    if((slaObj.keyPreStatus == -1 || slaObj.keyPreStatus == 3 || slaObj.keyPreStatus == 7)&& status !=1 && status !=2){
        slaObj.finishTime = "暂时未知";
    }
    var row = "<tr class = " + addClass +">";
    row += "<td>"+ slaObj.slaId +"</td>";
    row += "<td>"+ typedesc +"</td>";
    row += "<td>"+ slaObj.slaName +"</td>";
    row += "<td>"+ slaObj.finishTime +"</td>";
    row += "<td>"+ statdesc +"</td>";
    row += "<td style='max-width:250px;overflow: hidden'>"+ slaObj.keyPreTaskName +"</td>";
    row += "<td>"+ statusMap(slaObj.keyPreStatus) +"</td>";
    row += "<td>"+ slaObj.warnTime +"</td>";
    row += "<td style='text-align: center'>"+ Math.round(slaObj.jobValue*100)/100 +"</td>";
    row += "</tr>";
    return row;
}

function dataPresent(datas) {
    var jArray = datas;
    var tableContent = "";
    var rowData = "";
    jArray.sort(sortSLA);
    var delayJobs = 0;
    var delayValue = 0;
    var jobNum = jArray.length;
    for(var i=0; i < jArray.length; i++){
        var slaObj = jArray[i];
        if(slaObj.reportStatus == -1 || slaObj.reportStatus == 2){
            delayJobs++;
            delayValue += slaObj.jobValue;
        }
        rowData += createRow(slaObj);
    }
    var accType =
        delayValue < 100 ? '无' :
        delayValue < 200 ? 'P5' :
        delayValue < 400 ? 'P4' :
        delayValue < 600 ? 'P3' :
        delayValue < 800 ? 'P2' :
        'P1';
    var succRate = 100*(jobNum - delayJobs)/jobNum;
    var staticContent = "当前SLA任务总数为<font color='green'>"+jobNum+"</font>, 其中延迟任务数为<font color='red'>"+delayJobs+"</font>, 当前按时完成率为"+succRate.toFixed(2)+"%";
    staticContent += "; 当前总故障分为 <font color = 'red'>"+Math.round(delayValue*100)/100+"</font>, ";
    if(accType == "无")
        staticContent += "无故障, ";
    else{
        staticContent += "故障等级(测试): <font color = 'red'>"+accType+", </font>";
    }
    staticContent += "<a id= 'fail-anay' clicked = 0 type = 'button' class='btn btn-primary btn-lg' onclick='createReport(this)'>延迟原因分析</a>"
    tableContent += rowData;
    tableContent += "";
    $("#result-body").html('');
    $("#result-body").append(tableContent);
    $("#sla_static").html('');
    $("#sla_static").append(staticContent);

}

function sortSLA(a,b){
    var aStat = a.reportStatus;
    var bStat = b.reportStatus;
    if(aStat == 2){
         aStat = -0.5;
    }
    if(bStat == 2){
        bStat = -0.5;
    }
    if(aStat != bStat){
        return aStat - bStat;
    }
    else{
        return a.slaId - b.slaId;
    }
}

function initColumnInfo(data) {
    var responseObj = data;
    if(responseObj.code == 200){
        dataPresent(responseObj.msg);
    }
    else{
        alert("无SLA数据");
    }
}
function getSlaData(){
    $.ajax({
        async: false,
        cache: false,
        type: 'POST',
        dataType: "json",
        url: "json/getSlaJobs?date="+slaPageViewDate,  //请求搜索的路径
        timeout: 5000,
        error: function () {              //请求失败处理函数
            alert("获取数据出错！");
        },

        success: function (data) {
            initColumnInfo(data);
        }
    });

}

function createReport(obj){
    if(!$("#fail-cotent").is(":hidden")){
        $("#fail-cotent").hide();
        return;
    }
    var $me = $(obj);
    if($me.attr('clicked') == '1'){
        return;
    }
    $.ajax({
        async: false,
        cache: false,
        type: 'POST',
        dataType: "json",
        url: "json/getBottleNecks?date="+slaPageViewDate,  //请求搜索的路径
        timeout: 50000,
        error: function () {              //请求失败处理函数
            alert("无可用故障分析");
        },
        beforeSend:function(){
            $me.attr("clicked", '1');
        },
        success: function (data) {
            var responseObj = data;
            if(responseObj.code == 200){
                createFailureContent(responseObj.msg);
            }
            else{
                alert("无可用故障分析");
            }
        },
        complete: function(){
            $me.attr("clicked", '0');
        }
    });
}
function createFailureContent(datas){
    $("#hiddendivs").html('');
    var jArray = datas;
    var abnormalContent = "";
    var normalContent = "";
    $("#abnormal-body").html('');
    $("#normal-body").html('');
    var statusDetail = createStatusBar();
    for(var i=0; i < jArray.length; i++){
        var slaObj = jArray[i];
        if(slaObj.delaySec<3000)
            break;
        var detailHtml="<table class = 'table table-hover'><tbody>";
        for(var j=0;j<statusDetail.length;j++){
            var tempObj = statusDetail[j];
            if(tempObj.key_task_id == slaObj.keyTaskId){
                detailHtml += "<tr><td>"+statusMap(tempObj.key_task_status)+"</td><td>"+timetransform(tempObj.block_time) +
                    "</td></tr>"
                statusDetail.splice(j,1);
                j--;
            }
        }
        detailHtml += "</tbody></table>";
        if(slaObj.abnormal == 1){
            $("#abnormal-body").append(createFailRow(slaObj, escape(detailHtml)));
        }
        else{
            $("#normal-body").append(createFailRow(slaObj, escape(detailHtml)));
        }
        $("#hiddendivstatusDetail" + slaObj.keyTaskId).html(detailHtml);
        var targetId = "hiddendiv" + slaObj.keyTaskId;
        $("#statusDetail" + slaObj.keyTaskId).popover({
            html:true,
            content: function(){
                return $("#hiddendiv"+$(this).attr("id")).html();
            }
        })
    }
    $("#fail-cotent").slideToggle("fast")

}

function createFailRow(slaObj,detailHtml) {
    detailHtml;
    var addClass = "";
    if(slaObj.abnormal == 1)
        addClass = "error"
    else
        addClass = "warning"
    var row = "<tr class = " + addClass +">";
    row += "<td>"+ slaObj.keyTaskId +"</td>";
    row += "<td>"+ slaObj.taskName +"</td>";
    row += "<td>"+ slaObj.owner +"</td>";
    row += "<td>"+ timetransform(slaObj.delaySec) +
        "<a id = statusDetail"+slaObj.keyTaskId+"  class='btn btn-mini btn-link' " +
        "data-toggle='popover'  data-original-title='状态详情'><i class='icon-chevron-down'></i></a> </td>";
    row += "<td>"+ slaObj.delayCnt +"</td>";
    row += "<td>"+ Math.round(slaObj.jobValue*100)/100 +"</td></tr>";
    $("#hiddendivs").append("<div id=hiddendivstatusDetail"+slaObj.keyTaskId+"></div>");
    return row;
}
function timetransform(second){
    var hour = Math.floor(second/3600);
    if(hour<10){
        hour = "0"+hour;
    }
    var min = Math.round((second%3600)/60);
    if(min<10){
        min = "0"+min;
    }
    return hour + " 小时" +min +" 分钟"
}
function getformattedDate(){
    var now=new Date()
    y=now.getFullYear()
    m=now.getMonth()+1
    d=now.getDate()
    m=m<10?"0"+m:m
    d=d<10?"0"+d:d
    return y+"-"+m+"-"+d
}

function createStatusBar(){
    var retrunData;
    $.ajax({
        async: false,
        cache: false,
        type: 'POST',
        dataType: "json",
        url: "json/getTaskStatusDetail?date="+slaPageViewDate,  //请求搜索的路径
        timeout: 5000,
        error: function () {              //请求失败处理函数
            return;
        },
        success: function (data) {
            var responseObj = data;
            if(responseObj.code == 200){
                retrunData =  data.msg

            }
            else{
                return;
            }
        }
    });
    return retrunData;
}

function escape(html) {
    var elem = document.createElement('div');
    var txt = document.createTextNode(html);
    elem.appendChild(txt);
    return elem.innerHTML;
}