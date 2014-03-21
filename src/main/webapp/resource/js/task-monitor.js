var userVerify = new Global.UserVerify();
var _zTreeObj;

var createBreadcrumb = function() {
    new FC.Breadcrumb($('#bread-crumb'), {
        siteStruction : {
            secondLevel : {
                title : '调度系统',
                subSites : [ {
                    title : '调度监控',
                    url : 'task-monitor.html'
                }, {
                    title : '查看任务',
                    url : 'task-view.html'
                }, {
                    title : '新增任务',
                    url : 'task-add.html'
                }, {
                    title : 'AutoETL',
                    url : 'autoetl-query.html'
                }, {
                    title : 'Hive建表工具',
                    url : 'hive-tool.html'
                },{
                    title:'SLA实时监控',
                    url: 'slaJob-status.html'
                } ]
            }
        },
        subSiteIndex : 0
    });
};

var _topology = null;
var _hasPowerRerun = false;
var _hasPowerSuspend = false;
var _hasPowerSuccess = false;

//拓扑图配置项
var _topologyOptions = {
    layerHeight: 80,
    colorMap: {
        '-1': '#FF3030',
        '1': '#458B00',
        '0': '#E3E3E3',
        '2': '#63B8FF',
        '3': '#878787',
        '4': '#EEEE00',
        '5': '#EEAD0E',
        '6': '#FFFCC7',
        '7': '#0000FF'
    },
    edgeColor: {
        error: '#c0c0c0',
        normal: '#c0c0c0'
    },
    singleMax: 10,
    keepLayerWhenDrag: true,
    layerHeight: 150,
    click: function(e, item) {
        if($('.info-dialog[node-id='+item.id+']').length > 0) {
            return;
        }

        $('#info-dialog').remove();

        _topology.forEach(function(node) {
            node.view.attr({
                'stroke': '#C09853',
                'stroke-width': '0px'
            });
        });

        var eventTarget = e.srcElement ? e.srcElement : e.target; //兼容火狐、IE

        var $infoDiv = $('<div id="info-dialog"/>')
            .css({
                'top': $(eventTarget).offset().top + 'px',
                'left': ($(document).width() - $(eventTarget).offset().left < 780
                    ? $(eventTarget).offset().left - 740
                    : $(eventTarget).offset().left) + 'px'
            }).attr('node-id', item.id).appendTo($(document.body));
        getTaskInfo(item, $infoDiv);
        item.view.attr({ 'stroke-width': '4px' });
        e.stopPropagation();
    }
};

var searchTopology = function() {
    var startDate = $('#start-date').val().trim();
    var endDate = $('#end-date').val().trim();
    var taskName = $('#task-name').val().trim();
    var cycle = $('#cycle').val().trim();
    var owner = $('#owner').val().trim();
    var status = $('#status').val().trim();
    var onlyself = $('#onlyself').val().trim();
    var prioLvl = $('#prioLvl').val().trim();

    if (startDate > endDate) {
        new FC.MessageLoader($('.back-info:first'), {
            msg: '开始时间晚于结束时间，请重新选择！',
            msgClassName: 'alert-error',
            keepOnly: true
        });
    } else if (startDate != endDate && !(/^\d+(\s*,\s*\d+)*$/g).test(taskName)) {
        new FC.MessageLoader($('.back-info:first'), {
            msg: '选择时间区段需填写任务ID，以逗号分隔，请重新选择！',
            msgClassName: 'alert-error',
            keepOnly: true
        });
    } else {
        $.ajax({
            url: '/darkbat/json/searchTaskRelaStatusAction',
            type: 'post',
            data: {
                startDate: startDate,
                endDate: endDate,
                taskName: taskName,
                cycle: cycle,
                owner: owner,
                status: status,
                onlyself: onlyself,
                prioLvl:prioLvl
            },
            dataType: 'json',
            success: function(cb) {
                setTimeout(function() {
                    refreshTopology(startDate, endDate, cb);
                }, 300);
            }
        });
    }
};

var keydownTrigger = function(event) {
    if (event.which == 13) {
        searchTopology();
    }
};

$("#start-date").keydown(keydownTrigger);
$("#end-date").keydown(keydownTrigger);
$("#status").keydown(keydownTrigger);
$("#owner").keydown(keydownTrigger);
$("#cycle").keydown(keydownTrigger);
$("#task-name").keydown(keydownTrigger);

$(document).click(function() {
    var $dialog = $('#info-dialog');
    if($dialog.length == 0)
        return;
    $dialog.remove();
});

$(function() {
    userVerify.verifyLogin(function() {
        _hasPowerRerun = userVerify.isDWTeam();
        _hasPowerSuccess = userVerify.isDWTeam();
        _hasPowerBatchstop = userVerify.isDWTeam();
        _hasPowerSuspend = userVerify.isDWTeam();
        //_hasPowerSuspend = userVerify.getPinyinName() === 'hongdi.tang';
        _hasPowerRaisePrority = userVerify.getPinyinName() === 'hongdi.tang' || userVerify.getPinyinName() === 'hong.zhao';
        createBreadcrumb();
        comm.addOnDutyInfo();

        bindSearch();

        var $datepicker = $('.datepicker').val(new Date().format('yyyy-MM-dd'));
        $datepicker.datepicker({
            format: 'yyyy-mm-dd',
            weekStart: 0
        });

        initTopology($datepicker.val(), $datepicker.val());
    });
    for (var i = 0; i < Global.AllTeam.length; ++i) {
        $('#owner').append($('<option>').attr('value', Global.AllTeam[i][0]).text(Global.AllTeam[i][1]));
    }
});

// 点击按钮查询
var bindSearch = function() {
    $('.btn-search').unbind('click').click(function() {
        searchTopology();
        return false;
    });
};

var refreshTopology = function(startDate, endDate, callBack) {
    var $info = $('.back-info:first');
    $info.html('');
    if(callBack.code == 500) {
        new FC.MessageLoader($info, {
            msgClassName: 'alert-error',
            msg: callBack.msg
        });
    } else if (callBack.code == 202) {
        new FC.MessageLoader($info, {
            msg: (function () {
                return '无所选条件的相关任务依赖信息';
            })(),
            msgClassName: 'alert-warn'
        });
    } else if (callBack.code == 200) {
        if (callBack.msg.length == 0) {
            new FC.MessageLoader($info, {
                msg: (function () {
                    return '无所选条件的相关任务依赖信息';
                })(),
                msgClassName: 'alert-warn'
            });
        } else {
            new FC.MessageLoader($info, {
                msg: (function () {
                    return '以下为 ' + startDate + ' 至 ' + endDate + ' 符合查询条件的任务依赖信息, 共 ' + callBack.msg.length + ' 个任务';
                })(),
                msgClassName: 'alert-success'
            });

            _topology = new Venus.Topology($('#topology-container').html('').get(0), callBack.msg, _topologyOptions);
            return;
        }
    }
    $('#topology-container').get(0).innerHTML = '';
};

//首次获取拓扑图
var initTopology = function(startDate, endDate) {
    $.ajax({
        url: '/darkbat/json/searchTaskRelaStatusAction',
        type: 'post',
        data: {
            startDate: startDate,
            endDate: endDate,
            status: -2
        },
        dataType: 'json',
        success: function(cb) {
            setTimeout(function() {
                refreshTopology(startDate, endDate, cb);
            }, 300);
        }
    });
};

var getTaskInfo = function(item, $container) {
    $.ajax({
        url: '/darkbat/json/getTaskStatusAction',
        type: 'post',
        data: {
            taskStatusId: item.id
        },
        dataType: 'json',
        beforeSend: function() {
            $container.addClass('loading');
        },
        success: function(cb) {

            setTimeout(function() {
                $container.removeClass('loading');

                var canSuspend = (function(id) {
                    return (id === 0 || id === 6 || id === 7 || id === 5 || id ===-1 ) && _hasPowerSuspend;
                })(cb.msg[0].status);

                //是否可以重跑
                var canRerun = (function(id) {
                    return ((id === 1 || id === -1 || id === 3 || id === 5) && _hasPowerRerun);
                })(cb.msg[0].status);

                //是否可以改成功
                var canSuccess = (function(id) {
                    return ((id !== 1) && _hasPowerSuccess);
                })(cb.msg[0].status);

                var canBatchstop = (function(id){
                    return (cb.msg[0].task_status_id.substr(0,4)==="pre_" && _hasPowerBatchstop);
                })(cb.msg[0].task_status_id);

                var canRaisePriority = (function(id){
                    return ((id === 0 || id === 6) && _hasPowerRaisePrority);
                })(cb.msg[0].status);



                var html = ''
                    + ' <table class="table table-striped ">'
                    + '     <thead>'
                    + '            <tr>'
                    + '                <th width="25%">名称</th>'
                    + '                <th width="75%">值 </th>'
                    + '            </tr>'
                    + '        </thead>'
                    + '        <tbody>'
                    + '            <tr>'
                    + '                <td>任务实例ID</td>'
                    + '                <td>' + cb.msg[0].task_status_id + '</td>'
                    + '            </tr>'
                    + '            <tr>'
                    + '                <td>运行日期</td>'
                    + '                <td>' + cb.msg[0].time_id + '</td>'
                    + '            </tr>'
                    + '            <tr>'
                    + '                <td>任务名称</td>'
                    + '                <td>' + cb.msg[0].task_name + '</td>'
                    + '            </tr>'
                    + '            <tr>'
                    + '                <td>优先级</td>'
                    + '                <td>' + cb.msg[0].prio_lvl + '</td>'
                    + '            </tr>'
                    + '            <tr>'
                    + '                <td>任务日志</td>'
                    + '                <td><a href="task-log.html?logPath=' + cb.msg[0].log_path.replace(/#/g, '%23')
                    + '                " target="_BLANK">' + cb.msg[0].log_path + '</a></td>'
                    + '            </tr>'
                    + '            <tr>'
                    + '                <td>状态</td>'
                    + '                <td>' + getStatusNameById(cb.msg[0].status) + '</td>'
                    + '            </tr>'
                    + '            <tr>'
                    + '                <td>周期</td>'
                    + '                <td>' + getCycleNameById(cb.msg[0].cycle) + '</td>'
                    + '            </tr>'
                    + '            <tr>'
                    + '                <td>开发者</td>'
                    + '                <td>' + Global.getNameByPinyin(cb.msg[0].owner) + '</td>'
                    + '            </tr>'
                    + '            <tr>'
                    + '                <td>运行次数</td>'
                    + '                <td>' + cb.msg[0].run_num + '</td>'
                    + '            </tr>'
                    + '         <tr>'
                    + '                <td>重跑次数</td>'
                    + '                <td>' + cb.msg[0].recall_num + '</td>'
                    + '            </tr>'
                    + '            <tr>'
                    + '                <td>开始|结束时间</td>'
                    + '                <td>' + (cb.msg[0].start_time == undefined ? '-' : cb.msg[0].start_time.substring(0, 19)) + ' ~ ' + (cb.msg[0].end_time == undefined ? '-' : cb.msg[0].end_time.substring(0, 19)) + '</td>'
                    + '            </tr>'
                    + '        </tbody>'
                    + '    </table>'
                    + '    <div class="controller">'
                    + '        <ul>'
                    + (canRerun ? '<li><a class="btn btn-warning rerun">置为重跑</a></li>' : '')
                    + (canSuspend ? '<li><a class="btn btn-inverse suspend">置为挂起</a></li>' : '')
                    + (canSuccess ? '<li><a class="btn btn-success success">置为成功</a></li>' : '')
                    + (canRerun ? '<li><a class="btn btn-danger multi-rerun">级联重跑</a></li>' : '')
                    + ('<br><li><a class="btn btn-primary see-rela-simple">直接依赖</a></li>'
                    + '<li><a class="btn btn-primary see-rela-all">所有依赖</a></li>'
                    + '<li><a class="btn btn-primary see-longpath">最长路径</a></li>'
                    + '<li><a class="btn btn-primary" href="task-view.html?taskId=' + cb.msg[0].task_id + '" target="_BLANK">查看任务</a></li>')
                    + (canBatchstop && canSuspend ? '<li><a class="btn btn-warning batchstop">停止预跑</a></li>' : '')
                    + (canRaisePriority ? '<li><a class="btn btn-success raisePriority">快速通道</a></li>' : '')
                    + '        </ul>'
                    + '    </div>';

                html = '<div class="task-info clear-float-fix">' + html + '</div>';

                var $content = $(html);
                var argObj = {
                    taskName: cb.msg[0].task_name,
                    taskStatusId: cb.msg[0].task_status_id,
                    taskId: cb.msg[0].task_id,
                    node: item,
                    date: cb.msg[0].time_id,
                    owner: cb.msg[0].owner
                };
                if(canRerun) {
                    $content.find('.rerun').click(argObj, rerunTaskByTopology);
                    $content.find('.multi-rerun').click(argObj, getAllChildTasks);
                }
                if(canSuspend) {
                    $content.find('.suspend').click(argObj, SuspendTaskByTopology);
                }
                if(canSuccess) {
                    $content.find('.success').click(argObj, SuccessTaskByTopology);
                }
                if(canBatchstop) {
                    $content.find('.batchstop').click(argObj, BatchStopPrerunTasks);
                }
                if(canRaisePriority) {
                    $content.find('.raisePriority').click(argObj, raisePriority);
                }


                $content.find('.see-rela-simple').click({
                    args: argObj,
                    searchType: 'simple'
                }, searchTaskByTopology);

                $content.find('.see-rela-all').click({
                    args: argObj,
                    searchType: 'all'
                }, searchTaskByTopology);

                $content.find('.see-longpath').click({
                    args: argObj,
                    searchType: 'longpath'
                }, searchTaskByTopology);

                $content.find('table').click(function(e) {
                    e.stopPropagation();
                });

                $content.appendTo($container);
            }, 250);
        }
    });
};

/**
 * 点击拓扑图查询相关依赖
 */
var searchTaskByTopology = function(event) {
    var type = event.data.searchType;
    var url;
    if (type === 'simple') {
        url = '/darkbat/json/getSimplePostAndPreAction';
    } else if (type === 'all') {
        url = '/darkbat/json/getAllPreAndPostAction';
    } else if(type === 'longpath'){
        url = '/darkbat/json/getTimecostLongPath';
    }
    $.ajax({
        url: url,
        data: {
            taskStatusId: event.data.args.taskStatusId,
            date: event.data.args.date
        },
        dataType: 'json',
        type: 'post',
        success: function(cb) {
            if(cb.msg.length == 0) {
                new FC.MessageLoader($('.back-info:first') , {
                    msgClassName: 'alert-error',
                    msg: '任务' + event.data.args.taskName + '相关无依赖'
                });
            } else {
                new FC.MessageLoader($('.back-info:first') , {
                    msgClassName: 'alert-info',
                    msg: '以下为任务' + event.data.args.taskName + '相关依赖',
                    keepOnly: true
                });
                _topology = new Venus.Topology(
                    $('#topology-container').html('').get(0),
                    cb.msg,
                    _topologyOptions
                );
                _topology.getNodeById(event.data.args.taskStatusId).view.attr({
                    'stroke': '#C09853',
                    'stroke-width': '4px'
                });
            }
            $('#info-dialog').remove();
        }
    });
};

/**
 * 点击重跑触发事件
 */
var rerunTaskByTopology = function(event) {
    var $that = $(this);
    new FC.ConfirmBox($that, {
        msg: '重跑任务' + event.data.taskName + '<br/>开发者：' + Global.getNameByPinyin(event.data.owner) + '<br/>确定执行？',
        doConfirm: function(arg) {
            $.ajax({
                url: 'json/rerunTaskAction',
                data: {
                    taskStatusId: arg.taskStatusId
                },
                type: 'post',
                dataType: 'json',
                success: function(cb) {
                    if(cb.code == 200) {
                        setTimeout(function() {
                            arg.node.view.attr({
                                fill: '#E3E3E3'
                            });
                            new FC.MessageLoader($('.back-info:first') , {
                                msgClassName: 'alert-info',
                                msg: '任务' + arg.taskName + '进入重跑状态',
                                keepOnly: true
                            });
                        }, 1000);
                    }
                    $('#info-dialog').remove();
                }
            });
        },
        eventData: {
            taskStatusId: event.data.taskStatusId,
            taskName: event.data.taskName,
            node: event.data.node
        }
    });

    event.stopPropagation();
};

/**
 * 挂起任务
 */
var SuspendTaskByTopology = function(event) {
    var $that = $(this);
    new FC.ConfirmBox($that, {
        msg: '挂起任务' + event.data.taskName + '<br/>开发者：' + Global.getNameByPinyin(event.data.owner) + '<br/>确定执行？',
        doConfirm: function(arg) {
            $.ajax({
                url: 'json/modifyTaskAction',
                data: {
                    taskStatusId: arg.taskStatusId,
                    allowStatus: '0',
                    status: 3
                },
                type: 'post',
                dataType: 'json',
                success: function(cb) {
                    if(cb.code == 200) {
                        setTimeout(function() {
                            arg.node.view.attr({
                                fill: '#878787'
                            });
                            new FC.MessageLoader($('.back-info:first') , {
                                msgClassName: 'alert-info',
                                msg: '任务' + arg.taskName + '进入挂起状态',
                                keepOnly: true
                            });
                        }, 1000);
                    }
                    $('#info-dialog').remove();
                }
            });
        },
        eventData: {
            taskStatusId: event.data.taskStatusId,
            taskName: event.data.taskName,
            node: event.data.node
        }
    });

    event.stopPropagation();
};

/**
 * 挂起任务
 */
var BatchStopPrerunTasks = function(event) {
    var $that = $(this);
    new FC.ConfirmBox($that, {
        msg: '挂起任务' + event.data.taskName + '<br/>开发者：' + Global.getNameByPinyin(event.data.owner) + '<br/>确定执行？',
        doConfirm: function(arg) {
            $.ajax({
                url: 'json/batchstopPrerunTasks',
                data: {
                    taskId: event.data.taskId
                },
                type: 'post',
                dataType: 'json',
                success: function(cb) {
                    if(cb.code == 200) {
                        setTimeout(function() {
                            arg.node.view.attr({
                                fill: '#878787'
                            });
                            new FC.MessageLoader($('.back-info:first') , {
                                msgClassName: 'alert-info',
                                msg: '所有预跑任务' + arg.taskName + '进入挂起状态',
                                keepOnly: true
                            });
                        }, 1000);
                    }
                    $('#info-dialog').remove();
                }
            });
        },
        eventData: {
            taskStatusId: event.data.taskStatusId,
            taskName: event.data.taskName,
            node: event.data.node
        }
    });

    event.stopPropagation();
};

/**
 *
 */
var raisePriority = function(event) {
    var $that = $(this);
    new FC.ConfirmBox($that, {
        msg: '提高优先级' + event.data.taskName + '<br/>开发者：' + Global.getNameByPinyin(event.data.owner) + '<br/>确定执行？',
        doConfirm: function(arg) {
            $.ajax({
                url: 'json/raisePriorityAction',
                data: {
                    taskStatusId: event.data.taskStatusId
                },
                type: 'post',
                dataType: 'json',
                success: function(cb) {
                    if(cb.code == 200) {
                        setTimeout(function() {
                            new FC.MessageLoader($('.back-info:first') , {
                                msgClassName: 'alert-info',
                                msg: arg.taskName + '优先级已经提高',
                                keepOnly: true
                            });
                        }, 1000);
                    }
                    $('#info-dialog').remove();
                }
            });
        },
        eventData: {
            taskStatusId: event.data.taskStatusId,
            taskName: event.data.taskName,
            node: event.data.node
        }
    });

    event.stopPropagation();
};

/**
 * 任务级联重跑
 */
var getAllChildTasks = function(event) {
    $.ajax({
        url: 'json/getAllChildren',
        data: {
            taskStatusId: event.data.taskStatusId,
            date: event.data.date
        },
        type: 'post',
        dataType: 'json',
        async: false,
        success: function(cb) {
            if(cb.code == 200) {
                popAllChildTree();

                var $datepicker = $('#rerun-start-date').val(new Date().format('yyyy-MM-dd'));
                $datepicker.datepicker({
                    format: 'yyyy-mm-dd',
                    weekStart: 0
                });

                var $datepicker = $('#rerun-end-date').val(new Date().format('yyyy-MM-dd'));
                $datepicker.datepicker({
                    format: 'yyyy-mm-dd',
                    weekStart: 0
                });

                var $tree = $('#tree');
                var treeId = 'tree';
                var treeSetting = {
                    treeId: treeId,
                    view: {
                        selectedMulti: false
                    },
                    data: {
                        simpleData: {
                            enable: true
                        }
                    },
                    callback: {
                        onCheck: function(event, treeId, treeNode) {

                        }
                    },
                    check: {
                        enable: true,
                        chkStyle: 'checkbox',
                        chkboxType: {
                            'Y': 'ps',
                            'N': 's'
                        }
                    }
                };
                var nodes = initChildNodes(cb.msg, {
                    allChecked: true
                });

                _zTreeObj = $.fn.zTree.init($tree, treeSetting, nodes);
                _zTreeObj.expandAll(true);
            }
        }
    });

    event.stopPropagation();
};

/**
 * 挂起任务
 */
var SuccessTaskByTopology = function(event) {
    var $that = $(this);
    new FC.ConfirmBox($that, {
        msg: '将任务置成功' + event.data.taskName + '<br/>开发者：' + Global.getNameByPinyin(event.data.owner) + '<br/>确定执行？',
        doConfirm: function(arg) {
            $.ajax({
                url: 'json/modifyTaskAction',
                data: {
                    taskStatusId: arg.taskStatusId,
                    allowStatus: '1',
                    status: 1
                },
                type: 'post',
                dataType: 'json',
                success: function(cb) {
                    if(cb.code == 200) {
                        setTimeout(function() {

                            //改变颜色
                            arg.node.view.attr({
                                fill: '#458B00'
                            });

                            new FC.MessageLoader($('.back-info:first') , {
                                msgClassName: 'alert-info',
                                msg: '任务' + arg.taskName + '进入成功状态',
                                keepOnly: true
                            });
                        }, 1000);
                    }
                    $('#info-dialog').remove();
                }
            });
        },
        eventData: {
            taskStatusId: event.data.taskStatusId,
            taskName: event.data.taskName,
            node: event.data.node
        }
    });

    event.stopPropagation();
};

var initChildNodes = function(nodeArr, options) {
    for(var i = 0; i < nodeArr.length; i++) {
        if(options.allChecked)
            nodeArr[i].checked = true;
    }
    return nodeArr;
};

var popAllChildTree = function() {
    new FC.BindShowDialog(null, {
        width: 960,
        maxHeight: 460,
        bodyHtml: (function() {
            var htmlArr = [];
            htmlArr.push('<div id="main-content-tree"><div id="tree-back-info">');
            htmlArr.push('<div> 开始时间  <input type="text" class="datepicker" id="rerun-start-date"/>');
            htmlArr.push(' 结束时间 <input type="text" class="datepicker" id="rerun-end-date"/></div>');
            htmlArr.push('<ul class="ztree" id="tree"/></div></div>');

            return htmlArr.join('');
        })(),
        title: '级联重跑',
        dialogId: 'multi-rerun',
        backdrop: 'static',
        show: true
    }, {
        submit: function() {
            var checkedNodes = _zTreeObj.getCheckedNodes(true);
            var unCheckedNodes = _zTreeObj.getCheckedNodes(false);
            var conflictCheckedNodes = [];
            var conflictUnCheckedNodes = [];

            for(var i = 0; i < checkedNodes.length; i++) {
                if($.inArray(checkedNodes[i], conflictCheckedNodes) === -1) {
                    for(var j = 0; j < unCheckedNodes.length; j++) {
                        if(checkedNodes[i].id === unCheckedNodes[j].id) {
                            conflictCheckedNodes.push(checkedNodes[i]);
                            conflictUnCheckedNodes.push(unCheckedNodes[j]);
                            break;
                        }
                    }
                }
            }

            if(conflictCheckedNodes.length > 0) {
                new FC.MessageLoader($('#tree-back-info'), {
                    msg: '指向相同的子任务，重跑与否选项发生冲突',
                    msgClassName: 'alert-error',
                    keepOnly: true
                });
                var className = 'button ' + 'error_ico_docu';
                var changeNodeClass = function(node, toClassName) {
                    var $e = $('#' + node.tId + '_ico');
                    $e.attr('class', toClassName);
                };
                for(var i = 0;i < conflictCheckedNodes.length; i++) {
                    changeNodeClass(conflictCheckedNodes[i], className);
                    changeNodeClass(conflictUnCheckedNodes[i], className);
                }
            } else {
                var startDate = $('#rerun-start-date').val().trim();
                var endDate = $('#rerun-end-date').val().trim();

                if (startDate > endDate) {
                    new FC.MessageLoader($('#tree-back-info'), {
                        msg: '开始时间晚于结束时间，请重新选择',
                        msgClassName: 'alert-error',
                        keepOnly: true
                    });
                } else {
                    var ids = (function(nodes) {
                        var idArr = [];
                        $(nodes).each(function(i, e) {
                            idArr.push(e.id.slice(0, -10));
                        });

                        return "'" + idArr.join("', '") + "'";
                    })(checkedNodes);

                    $.ajax({
                        url: 'json/rerunMultiJobs',
                        data: {
                            ids: ids,
                            startDate: startDate,
                            endDate: endDate
                        },
                        type: 'post',
                        dataType: 'json',
                        success: function(cb) {
                            if(cb.code == 200 && cb.msg >= 0) {
                                new FC.MessageLoader($('.back-info:first'), {
                                    msg: cb.msg + '个任务被重跑',
                                    msgClassName: 'alert-info',
                                    keepOnly: true
                                });

                                $('#multi-rerun').remove();
                                $('.modal-backdrop').remove();

                                setTimeout(function() {
                                    $(checkedNodes).each(function(i, e) {
                                        _topology.getNodeById(e.id.toString()).view.attr({
                                            'fill': '#E3E3E3'
                                        });
                                    });
                                }, 500);
                            }
                        }
                    });
                }
            }
        }
    });
};
