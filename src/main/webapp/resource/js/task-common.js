var getTimeoutNameById = function(id) {
    switch (id) {
    case 90:
        return "1.5小时";
    case 120:
        return "2小时";
    case 150:
        return "2.5小时";
    default:
        return id + "分钟";
    }
};

var comm = {};

/**
 * 面包屑添加值班人员
 */
comm.addOnDutyInfo = function() {
    var $div = $('#bread-crumb');
    $.ajax({
       url: 'json/getCurrentMonitorUser',
       type: 'get',
       dataType: 'json'
    })
    .done(function(cb) {
        if (cb && cb.code === 200) {
            var name = cb.msg.userName,
                officeNo = cb.msg.officeNo,
                mobileNo = cb.msg.mobileNo;
            officeNo = officeNo === '0' ? '' : officeNo;
            $div.append(
                    '<div class="on-duty-list">本周值班: ' + name + ' ' + officeNo + ' ' + mobileNo + '</div>');
        } else {
            $div.append(
                    '<div class="on-duty-list" style="color: red;">获取值班信息失败</div>'
            );
        }
    })
    .fail(function(cb) {
        $div.append(
                '<div class="on-duty-list" style="color: red;">获取值班信息失败</div>'
        );
    });
};

function isEmptyInput($container) {
	if($container) {
		var val = $container.val().trim();
		
		return (val === '');
	}
	return true;
}


var getStatusNameById = function(id) {
    switch (id) {
    case -2:
        return "unsuccess";
    case -1:
        return "fail";
    case 0:
        return "init";
    case 1:
        return "success";
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
        return "unknown";
    }
};

var getTaskGroupNameById = function(id) {
    var taskGroupArray = ['unknown', 'wormhole', 'mid/dim', 'dm', 'rpt', 'mail', 'dw', 'DQ', 'atom'];
    return id < taskGroupArray.length ? taskGroupArray[id] : 'unknown';
};

var getTypeNameById = function(id) {
    switch (id) {
    case 1:
        return "wormhole";
    case 2:
        return "calculate";
    default:
        return "unknown";
    }
};

var getPrioLvlNameById = function(id) {
    switch (id) {
    case 1:
        return "高";
    case 2:
        return "中";
    case 3:
        return "低";
    default:
        return "unknown";
    }
};

var getIfValNameById = function(id) {
    switch (id) {
    case 0:
        return "否";
    case 1:
        return "是";
    case 2:
        return "失效";
    default:
        return "unknown";
    }
};

var getIfWaitNameById = function(id) {
    switch (id) {
    case 0:
        return "否";
    case 1:
        return "是";
    default:
        return "unknown";
    }
};

var getIfRecallNameById = function(id) {
    switch (id) {
    case 0:
        return "否";
    case 1:
        return "是";
    default:
        return "unknown";
    }
};

var getCycleNameById = function(id) {
    id = id.toUpperCase();
    if (id == "Y")  return "年";
    if (id == "S")  return "季";
    if (id == "M")  return "月";
    if (id == "W")  return "周";
    if (id == "D")  return "日";
    if (id == "H")  return "时";
    if (id == "MI") return "分";
    else            return id;
};

var isOwnerOrAdmin = function(owner) {
    var pinyin = userVerify.getPinyinName();
    return owner === pinyin || userVerify.isAdminTeam(owner);
};

var getTaskDetailById = function(event) {
    $.ajax({
        url: "task-detail.html",
        dataType: 'html',
        async: false,
        type: 'get'
    }).done(function(data) {
        $('#task-detail-modal').html(data).modal({
            keyboard: false,
            backdrop: 'static'
        }).css({
            width: '1000px',
            'margin-left': function () {
                return -($(this).width() / 2);
            },
            'max-height': '90%',
            'margin-top': function() {
                if ($.browser.msie) {
                    return '-23%';
                } else if ($.browser.mozilla) {
                    return '-23%';
                }
                return '-23%';
            }
        }).show();
        $.ajax({
            url: '/darkbat/json/getTaskDetailAction',
            type: 'post',
            async: false,
            dataType: 'json',
            data: { taskId: event.data.taskId }
        }).done(function(data) {
            setTimeout(function() {
                $('#task-id').val(data.msg.taskId);
                
                var basic = $('#basic');

                for (var i = 0; i < Global.AllTeam.length; ++i) {
                    basic.find('[name=owner]').append($('<option>').attr('value', Global.AllTeam[i][0]).text(Global.AllTeam[i][1]));
                }

                basic.find('[name=owner]').val(data.msg.owner);
                basic.find('[name=database-src]').val(data.msg.databaseSrc);
                basic.find('[name=table-name]').val(data.msg.tableName);
                basic.find('[name=task-name]').val(data.msg.taskName);
                basic.find('[name=task-group-id]').val(data.msg.taskGroupId);
                basic.find('[name=cycle]').val(data.msg.cycle);
                basic.find('[name=timeout]').val(data.msg.timeout);
                basic.find('[name=prio-lvl]').val([data.msg.prioLvl]);
                basic.find('[name=if-val]').val([data.msg.ifVal]);
                basic.find('[name=if-val]').change(function(a,b,c) {
                    if (this.value != 1) {
                        alert('请务必确认此任务是否有依赖任务，并对其影响性进行评估，操作者将对此操作负有全责！');
                    }
                });
                basic.find('[name=offset-type]').val(data.msg.offsetType);

                var basic = $('#basic');
                if (data.msg.offsetType == 'offset') {
                    var offset = basic.find('[name=offset]');
                    var td = offset.parent();
                    offset.remove();
                    td
                        .append($('<select>')
                            .attr('class', 'input-big')
                            .attr('name', 'offset')
                            .append($('<option>').attr('value', 'D0').text('D0'))
                            .append($('<option>').attr('value', 'D1').text('D1'))
                            .append($('<option>').attr('value', 'D2').text('D2'))
                            .append($('<option>').attr('value', 'D3').text('D3'))
                            .append($('<option>').attr('value', 'D4').text('D4'))
                            .append($('<option>').attr('value', 'D5').text('D5'))
                            .append($('<option>').attr('value', 'D6').text('D6'))
                            .append($('<option>').attr('value', 'M0').text('M0'))
                            .append($('<option>').attr('value', 'M1').text('M1'))
                            .append($('<option>').attr('value', 'M2').text('M2'))
                            .append($('<option>').attr('value', 'M3').text('M3'))
                            .append($('<option>').attr('value', 'M4').text('M4'))
                            .append($('<option>').attr('value', 'M5').text('M5'))
                            .append($('<option>').attr('value', 'M6').text('M6'))
                        );
                } else if (data.msg.offsetType == 'appoint') {
                    var offset = basic.find('[name=offset]');
                    var td = offset.parent();
                    offset.remove();
                    td
                        .append(
                            $('<input>')
                                .attr('type', 'text')
                                .attr('name', 'offset')
                            );
                    offset = basic.find('[name=offset]');
                    new FC.DatePicker(offset, offset);
                }
                basic.find('[name=offset]').val(data.msg.offset);
                basic.find('[name=freq]').val(data.msg.freq);
                basic.find('[name=if-recall]').val([data.msg.ifRecall]);
                basic.find('[name=recall-code]').val(data.msg.recallCode);
                basic.find('[name=if-wait]').val([data.msg.ifWait]);
                basic.find('[name=wait-code]').val(data.msg.waitCode);
                basic.find('[name=success-code]').val(data.msg.successCode);
                basic.find('[name=recall-limit]').val(data.msg.recallLimit);
                basic.find('[name=recall-interval]').val(data.msg.recallInterval);
                basic.find('[name=para1]').val(data.msg.para1);
                basic.find('[name=para2]').val(data.msg.para2);
                basic.find('[name=para3]').val(data.msg.para3);
                basic.find('[name=remark]').val(data.msg.remark);

                $('#advanced-body').empty();
                var zeroToThirty = $('<select>').attr('name', 'dep-cycle-gap').attr('style', 'width:100%;');
                for (var i = 0; i < 31; ++i) {
                    zeroToThirty.append($('<option>').attr('value', i).text(i));
                }
                $.each(data.msg.preDepends, function() {
                    $('#advanced-body')
                        .append($('<tr>')
                            .append($('<td>').attr('style', 'width:20%;')
                                .append(getTaskGroupNameById(this.depTaskGroupId))
                            )
                            .append($('<td>').attr('style', 'width:50%;')
                                .append($('<select>').attr('name', 'dep-task-id').attr('style', 'width:100%;')
                                    .append($('<option>').attr('value', this.depTaskId).text(this.depTaskName)))
                            )
                            .append($('<td>').attr('style', 'width:20%;')
                                .append(zeroToThirty.clone())
                            )
                            .append($('<td>').attr('style', 'width:10%;')
                            )
                    );
                    var taskGroupTd = $('#advanced-body').find('tr:last td:first');
                    var taskNameTd = taskGroupTd.next();
                    var cycleGapTd = taskNameTd.next();
                    var cycleGapSelect = cycleGapTd.find('select:first');
                    cycleGapSelect.val(this.depCycleGap);
                    var buttonTd = cycleGapTd.next();
                    if (isOwnerOrAdmin(data.msg.owner)) {
                        buttonTd
                            .append($('<button>').attr('class', 'btn')
                                .append($('<i>').attr('class', 'icon-remove'))
                                .unbind('click').click(function() {
                                    $(this).parent().parent().remove();
                                })
                            );
                    }
                });

                if (data.msg.ifRecall == '0') {
                    basic.find('[name=recall-code]').attr('disabled', 'disabled').removeAttr('placeholder');
                }
                
                if (data.msg.ifWait == '0') {
                    basic.find('[name=wait-code]').attr('disabled', 'disabled').removeAttr('placeholder');
                }

                if (!isOwnerOrAdmin(data.msg.owner)) {
                    $('#submit-btn').attr('disabled', 'disabled');
                    $('#copy-btn').attr('disabled', 'disabled');
                    $('#advanced').find('tfoot').addClass('HIDDEN');
                } else {
                    if (data.msg.taskGroupId != 1) {
                        basic.find('[name=database-src]').empty()
                            .append($('<option>').attr('value', 'hive').text('hive'))
                            .append($('<option>').attr('value', 'gp57').text('gp57'))
                            .append($('<option>').attr('value', 'gp59').text('gp59'));
                        basic.find('[name=database-src]').val(data.msg.databaseSrc);
                        basic.find('[name=database-src]').change(function() {
                            basic.find('[name=task-name]').attr('readonly', true);
                            if (basic.find('[name=database-src]').val() + '##' + basic.find('[name=table-name]').val() != data.msg.taskName) {
                                checkTaskName();
                            } else {
                                basic.find('[name=table-name]').next().remove();
                                basic.find('[name=task-name]').next().remove();
                                basic.find('[name=table-name]').parents('.control-group').removeClass('success').removeClass('error');
                                basic.find('[name=task-name]').parents('.control-group').removeClass('success').removeClass('error');
                                basic.find('[name=task-name]').val(data.msg.taskName);
                            }
                        });
                        basic.find('[name=table-name]').blur(function() {
                            if (basic.find('[name=database-src]').val() + '##' + basic.find('[name=table-name]').val() != data.msg.taskName) {
                                checkTaskName();
                            } else {
                                basic.find('[name=table-name]').next().remove();
                                basic.find('[name=task-name]').next().remove();
                                basic.find('[name=table-name]').parents('.control-group').removeClass('success').removeClass('error');
                                basic.find('[name=task-name]').parents('.control-group').removeClass('success').removeClass('error');
                                basic.find('[name=task-name]').val(data.msg.taskName);
                            }
                        });
                        basic.find('[name=table-name]').attr('readonly', false);
                        basic.find('[name=task-group-id]').empty()
                            .append($('<option>').attr('value', '2').text('mid/dim'))
                            .append($('<option>').attr('value', '3').text('dm'))
                            .append($('<option>').attr('value', '4').text('rpt'))
                            .append($('<option>').attr('value', '5').text('mail'))
                            .append($('<option>').attr('value', '6').text('dw'))
                            .append($('<option>').attr('value', '7').text('DQ'))
                            .append($('<option>').attr('value', '8').text('atom'));
                        basic.find('[name=task-group-id]').val(data.msg.taskGroupId);
                    } else {
                        basic.find('[name=database-src]').empty()
                            .append($('<option>').attr('value', data.msg.databaseSrc).text(data.msg.databaseSrc));
                        basic.find('[name=database-src]').val(data.msg.databaseSrc);
                        basic.find('[name=task-group-id]').empty()
                            .append($('<option>').attr('value', '1').text('wormhole'));
                        basic.find('[name=task-group-id]').val(data.msg.taskGroupId);
                        basic.find('[name=table-name]').attr('readonly', true);
                    }
                    basic.find('[name=freq]').blur(checkFreq);
                    basic.find('[name=recall-code]').blur(checkRecallCode);
                    basic.find('[name=wait-code]').blur(checkWaitCode);
                    basic.find('[name=success-code]').blur(checkSuccessCode);
                    basic.find('[name=offset-type]').change(changeOffsetType);
                    basic.find('[name=if-recall]').change(changeIfRecall);
                    basic.find('[name=if-wait]').change(changeIfWait);

                    $('#submit-btn').click(function(event) {
                        addOrModifyTask();
                        event.preventDefault();
                    });

                    $('#copy-btn').attr('disabled', 'disabled');

                    $('#add-row-btn').click(openWin);
                    
                    $('#dol-btn').click(getDolInfo);
                    var index = basic.find('[name=para2]').val().indexOf('-dol');
                    if(index >= 0){
                        $('#dol-input').val(basic.find('[name=para2]').val().substring(index+4).trim());
                    }
                }

                $('#accordion').removeClass('HIDDEN');
            }, 100);
        });
    }).fail(function(jqXHR, textStatus) {
        new FC.MessageLoader($('#result-info'), {
            msgClassName: 'alert-error',
            msg: '系统出错,' + jqXHR.responseText,
            keepOnly: true
        });
        window.location.href = window.location.href.replace(/#.*/gi, '') + '#top';
    });
};

var deleteTaskById = function(event) {
    $('#delete-modal').modal().css({
        width: '300px',
        'margin-left': function () {
            return -($(this).width() / 2);
        },
        'margin-top': function () {
            return -($(this).height() / 2);
        }
    }).show();
    $('#delete-btn').unbind('click').click(function() {
        $('#delete-modal').modal('hide');
        $.ajax({
            url: '/darkbat/json/deleteTaskAction',
            type: 'post',
            async: false,
            dataType: 'json',
            data: { taskId: event.data.taskId }
        }).done(function(data) {
            if (data.code == 200) {
                new FC.MessageLoader($('#result-info'), {
                    msgClassName: 'alert-success',
                    msg: '删除成功',
                    keepOnly: true,
                    closeTime: 5000
                });
            } else {
                new FC.MessageLoader($('#result-info'), {
                    msgClassName: 'alert-error',
                    msg: '删除失败，' + data.msg,
                    keepOnly: true,
                    closeTime: 5000
                });
            }
            window.location.href = window.location.href.replace(/#.*/gi, '') + '#top';
        }).fail(function(jqXHR, textStatus) {
            new FC.MessageLoader($('#result-info'), {
                msgClassName: 'alert-error',
                msg: '删除失败，' + jqXHR.responseText,
                keepOnly: true
            });
            window.location.href = window.location.href.replace(/#.*/gi, '') + '#top';
        });
    });
};

var prerunJobById = function(event) {
    $('#prerun-modal').modal().css({
        width: '540px',
        'margin-left': function () {
            return -($(this).width() / 2);
        },
        'margin-top': function () {
            return -($(this).height() / 2);
        }
    }).show();
    $('#prerun-btn').unbind('click').click(function() {
        var $begin = $('#beginDate');
        var $end = $('#endDate');

        var beginDate = $begin.val();
        var endDate = $end.val();
        if(!beginDate && !endDate){
            //TODO
            new FC.MessageLoader($('#back-info'), {
                msgClassName: 'alert-error',
                msg: '开始时间和结束时间不能为空',
                keepOnly: true
            });
            return;
        }
        $('#prerun-modal').modal('hide');
        $.ajax({
            url: '/darkbat/json/prerunJob',
            type: 'post',
            async: false,
            dataType: 'json',
            data: {
                taskList: '['+event.data.taskId+']',
                begin : beginDate,
                end : endDate
            }
        }).done(function(data) {
                if (data.code == 200) {
                    new FC.MessageLoader($('#result-info'), {
                        msgClassName: 'alert-success',
                        msg: '生成预跑任务成功,请在监控界面查看',
                        keepOnly: true,
                        closeTime: 5000
                    });
                    $begin.val('');
                    $end.val('');
                } else {
                    new FC.MessageLoader($('#result-info'), {
                        msgClassName: 'alert-error',
                        msg: '预跑任务失败，' + data.msg,
                        keepOnly: true,
                        closeTime: 5000
                    });
                }
                window.location.href = window.location.href.replace(/#.*/gi, '') + '#top';
            }).fail(function(jqXHR, textStatus) {
                new FC.MessageLoader($('#result-info'), {
                    msgClassName: 'alert-error',
                    msg: '预跑任务失败，' + jqXHR.responseText,
                    keepOnly: true
                });
                window.location.href = window.location.href.replace(/#.*/gi, '') + '#top';
            });
    });
};


var refreshTable = function(data, usage, isWin) {
    var resultInfo = isWin == 0 ? $('#result-info') : $('#win-result-info');
    var resultBody = isWin == 0 ? $('#result-body') : $('#win-result-body');
    if (data.msg.count == 0) {
        new FC.MessageLoader(resultInfo, {
            msgClassName: 'alert-error',
            msg: '没有符合条件的结果',
            keepOnly: true,
            closeTime: 2500
        });
    }
    var tasks = data.msg.tasks;
    resultBody.empty();
    $.each(tasks, function() {
        var buttonTd = $('<td>');
        if (usage == 'task-add') {
            buttonTd.append($('<button>').attr('class', 'btn btn-mini').click({taskGroupId: this.taskGroupId, taskName: this.taskName, taskId: this.taskId}, addNewRow).append($('<i>').attr('class', 'icon-plus')));
        } else {
            if (isOwnerOrAdmin(this.owner)) {
                buttonTd.append($('<button>').attr('class', 'btn btn-mini').click({taskId: this.taskId}, getTaskDetailById).append($('<i>').attr('class', 'icon-edit')));
            } else {
                buttonTd.append($('<button>').attr('class', 'btn btn-mini').click({taskId: this.taskId}, getTaskDetailById).append($('<i>').attr('class', 'icon-list')));
            }
            if (isOwnerOrAdmin()) {
                buttonTd.append('&nbsp;');
                buttonTd.append($('<button>').attr('class', 'btn btn-mini').click({taskId: this.taskId}, deleteTaskById).append($('<i>').attr('class', 'icon-trash')));
            }
            if (isOwnerOrAdmin(this.owner)) {
                buttonTd.append('&nbsp;');
                buttonTd.append($('<button>').attr('class', 'btn btn-mini').click({taskId: this.taskId}, prerunJobById).append($('<i>').attr('class', 'icon-backward')));
            }

        }
        resultBody
            .append($('<tr>')
                .attr('id', 'tr-' + this.taskId)
                .append($('<td>').append(getTaskGroupNameById(this.taskGroupId)))
                .append($('<td>').append(this.taskId))
                .append($('<td>').append($('<div>').attr('style', 'text-align:left;').append(this.taskName)))
                .append($('<td>').append(getCycleNameById(this.cycle)))
                .append($('<td>').append(Global.getNameByPinyin(this.owner)))
                .append(buttonTd)
        );
    });
    resultBody.hide().fadeIn();
};

var selectorFormSearch = function(usage) {
    searchTask({
        taskId: '',
        taskGroupId: $('#selector-task-group-id').val(),
        cycle: $('#selector-cycle').val(),
        owner: $('#selector-owner').val(),
        databaseSrc: $('#selector-database-src').val(),
        ifVal: $('#selector-if-val').val(),
        taskName: $('#selector-task-name').val(),
        pageSize: 20,
        pageNo: 1,
        pageSort: '',
        usage: usage,
        isWin: 0
    });
};

var winSelectorFormSearch = function(usage) {
    searchTask({
        taskId: '',
        taskGroupId: $('#win-selector-task-group-id').val(),
        cycle: $('#win-selector-cycle').val(),
        owner: $('#win-selector-owner').val(),
        databaseSrc: $('#win-selector-database-src').val(),
        ifVal: $('#win-selector-if-val').val(),
        taskName: $('#win-selector-task-name').val(),
        pageSize: 20,
        pageNo: 1,
        pageSort: '',
        usage: usage,
        isWin: 1
    });
};

var searchTask = function(args) {
    var isWin = args.isWin;
    $.ajax({
        url: '/darkbat/json/searchTaskAction',
        async: false,
        type: 'post',
        dataType: 'json',
        data: {
            taskId: args.taskId,
            taskName: args.taskName,
            taskGroupId: args.taskGroupId,
            cycle: args.cycle,
            owner: args.owner,
            databaseSrc: args.databaseSrc,
            ifVal: args.ifVal,

            pageSize: args.pageSize,
            pageNo: args.pageNo,
            pageSort: args.pageSort
        }
    }).done(function(data) {
        var resultInfo = isWin == 0 ? $('#result-info') : $('#win-result-info');
        if (data.code != 200) {
            new FC.MessageLoader(resultInfo, {
                msgClassName: 'alert-error',
                msg: '系统出错，' + data.msg,
                keepOnly: true
            });
            window.location.href = window.location.href.replace(/#.*/gi, '') + '#top';
            return;
        }

        refreshTable(data, args.usage, isWin);
        var pagerEle1 = isWin == 0 ? $('#pager-ele1') : $('#win-pager-ele1');
        var pagerEle2 = isWin == 0 ? $('#pager-ele2') : $('#win-pager-ele2');
        new FC.Pager(pagerEle1, {
            currentPageNo: args.pageNo,
            lastPageNo: Math.ceil(data.msg.count/args.pageSize),
            onClick: function(pageNo) {
                searchTask({
                    taskId: args.taskId,
                    taskGroupId: args.taskGroupId,
                    cycle: args.cycle,
                    owner: args.owner,
                    databaseSrc: args.databaseSrc,
                    ifVal: args.ifVal,
                    taskName: args.taskName,
                    
                    pageSize: args.pageSize,
                    pageNo: pageNo,
                    pageSort: args.pageSort,
                    usage: args.usage,
                    isWin: args.isWin
                });
            }
        });
        new FC.Pager(pagerEle2, {
            currentPageNo: args.pageNo,
            lastPageNo: Math.ceil(data.msg.count/args.pageSize),
            onClick: function(pageNo) {
                searchTask({
                    taskId: args.taskId,
                    taskGroupId: args.taskGroupId,
                    cycle: args.cycle,
                    owner: args.owner,
                    databaseSrc: args.databaseSrc,
                    ifVal: args.ifVal,
                    taskName: args.taskName,
                    
                    pageSize: args.pageSize,
                    pageNo: pageNo,
                    pageSort: args.pageSort,
                    usage: args.usage,
                    isWin: args.isWin
                });
            }
        });
    }).fail(function(jqXHR, textStatus) {
        new FC.MessageLoader(resultInfo, {
            msgClassName: 'alert-error',
            msg: '系统出错,' + jqXHR.responseText,
            keepOnly: true
        });
        window.location.href = window.location.href.replace(/#.*/gi, '') + '#top';
    });
};

var openWin = function() {
    $.ajax({
        url: "task-search.html",
        dataType: 'html',
        async: false,
        type: 'get'
    }).done(function(data) {
        $('#task-search-modal').html(data).modal({
            keyboard: false,
            backdrop: 'static'
        }).css({
            width: '1200px',
            'margin-left': function () {
                return -($(this).width() / 2);
            },
            'max-height': '90%',
            'margin-top': function() {
                if ($.browser.msie) {
                    return '-22%';
                } else if ($.browser.mozilla) {
                    return '-22%';
                }
                return '-22%';
            }
        }).show();

        for (var i = 0; i < Global.AllTeam.length; ++i) {
            $('#win-selector-owner').append($('<option>').attr('value', Global.AllTeam[i][0]).text(Global.AllTeam[i][1]));
        }

        $('#win-search-btn').click(function(event) {
            winSelectorFormSearch('task-add');
            event.preventDefault();
        });
    }).fail(function(jqXHR, textStatus) {
        new FC.MessageLoader($('#win-result-info'), {
            msgClassName: 'alert-error',
            msg: '系统出错,' + jqXHR.responseText,
            keepOnly: true
        });
        window.location.href = window.location.href.replace(/#.*/gi, '') + '#top';
    });
};

var addNewRow = function(event) {
    var pTaskGroupId = parseInt(event.data.taskGroupId);
    var pTaskId = event.data.taskId;
    var pTaskName = event.data.taskName;
    var zeroToThirty = $('<select>').attr('name', 'dep-cycle-gap').attr('style', 'width:100%');
    for (var i = 0; i < 31; ++i) {
        zeroToThirty.append($('<option>').attr('value', i).text(i));
    }
    
    $('#advanced-body')
        .append($('<tr>')
            .append($('<td>')
                .attr('style', 'width:20%')
                .append(getTaskGroupNameById(pTaskGroupId))
            )
            .append(
                $('<td>').attr('style', 'width:50%')
                    .append(
                        $('<select>').attr('name', 'dep-task-id').attr('style', 'width:100%')
                            .append($('<option>').attr('value', pTaskId).text(pTaskName).attr('selected', 'selected')
                        )
                    )
            )
            .append($('<td>')
                .attr('style', 'width:20%')
                .append(zeroToThirty)
            )
            .append($('<td>')
                .attr('style', 'width:10%')
                .append($('<button>')
                    .attr('class', 'btn')
                    .append($('<i>').attr('class', 'icon-remove'))
                    .unbind('click').click(function() {
                        $(this).parent().parent().remove();
                    })
                )
            )
        );
    new FC.MessageLoader($('#win-result-info'), {
        msgClassName: 'alert-info',
        msg: '已添加: ' + event.data.taskName,
        keepOnly: true
    });
};

var addOrModifyTask = function() {
    var basic = $('#basic');
    var databaseSrc = basic.find('[name=database-src]');
    var tableName = basic.find('[name=table-name]');
    var tableNameVal = tableName.val();
    var taskGroupIdVal = basic.find('[name=task-group-id]').val();
    var tableNameControlGroup = tableName.parents('.control-group');
    if (taskGroupIdVal != 1) {
        var taskName = basic.find('[name=task-name]');
        var taskNameControlGroup = taskName.parents('.control-group');
        taskName.next().remove();
        taskNameControlGroup.removeClass('success').removeClass('error');
    }
    tableName.next().remove();
    tableNameControlGroup.removeClass('success').removeClass('error');
    if (tableNameVal == '') {
        tableNameControlGroup.addClass('error');
        tableName.parent().append($('<span>').attr('class', 'help-inline error').text('请填写结果表名'));
        return;
    } else if (taskGroupIdVal != 1 && tableNameVal.indexOf('.') == -1) {
        tableNameControlGroup.addClass('error');
        tableName.parent().append($('<span>').attr('class', 'help-inline error').text('格式应为数据库.表或模式.表'));
        return;
    }
    var form = $('#accordion');
    if (form.find('.control-group.error:first').length == 1) {
        var resultInfo;
        if ($('#task-detail-modal').length > 0) {
            resultInfo = $('#modal-result-info');
        } else {
            resultInfo = $('#result-info');
        }
        new FC.MessageLoader(resultInfo, {
            msgClassName: 'alert-error',
            msg: '请修改红色框中的字段',
            keepOnly: true,
            closeTime: 5000
        });
        window.location.href = window.location.href.replace(/#.*/gi, '') + '#top';
        return;
    }
    $.ajax({
        url: '/darkbat/json/addOrModifyTaskAction',
        type: 'post',
        dataType: 'json',
        data: {
            taskId: $('#task-id').val(),
            owner: form.find('[name=owner]').val(),
            databaseSrc: form.find('[name=database-src]').val(),
            tableName: form.find('[name=table-name]').val(),
            taskName: form.find('[name=task-name]').val(),
            taskGroupId : form.find('[name=task-group-id]').val(),
            cycle : form.find('[name=cycle]').val(),
            timeout : form.find('[name=timeout]').val(),
            prioLvl : form.find('[name=prio-lvl]:checked').val(),
            ifVal : form.find('[name=if-val]:checked').val(),
            offsetType : form.find('[name=offset-type]').val(),
            offset : form.find('[name=offset]').val(),
            freq : form.find('[name=freq]').val(),
            ifRecall: form.find('[name=if-recall]:checked').val(),
            recallCode : form.find('[name=recall-code]').val(),
            ifWait : form.find('[name=if-wait]:checked').val(),
            waitCode: form.find('[name=wait-code]').val(),
            successCode : form.find('[name=success-code]').val(),
            recallLimit : form.find('[name=recall-limit]').val(),
            recallInterval : form.find('[name=recall-interval]').val(),
            para1 : form.find('[name=para1]').val(),
            para1 : form.find('[name=para1]').val(),
            para2 : form.find('[name=para2]').val(),
            para3 : form.find('[name=para3]').val(),
            remark : form.find('[name=remark]').val(),
            depTaskGroupIds: form.find('[name=dep-task-group-id]').serialize(),
            depTaskIds: form.find('[name=dep-task-id]').serialize(),
            depCycleGaps: form.find('[name=dep-cycle-gap]').serialize(),
            addUser: userVerify.getUserEmail(),
            updateUser: userVerify.getUserEmail()
        }
    }).done(function(data) {
        if ($('#task-detail-modal').length > 0) {
            var taskTr = $('#tr-' + data.msg);
            if (taskTr) {
                var taskTd = taskTr.children().first();
                taskTd.html(getTaskGroupNameById(parseInt(form.find('[name=task-group-id]').val())));
                taskTd = taskTd.next().next();
                taskTd.html($('<div>').attr('style', 'text-align:left;').append(form.find('[name=task-name]').val()));
                taskTd = taskTd.next();
                taskTd.html(getCycleNameById(form.find('[name=cycle]').val()));
                taskTd = taskTd.next();
                taskTd.html(Global.getNameByPinyin(form.find('[name=owner]').val()));
            }
            $('#task-detail-modal').modal('hide');
        }
        if (data.code == 200) {
            new FC.MessageLoader($('#result-info'), {
                msgClassName: 'alert-success',
                msg: '新增或修改成功',
                keepOnly: true,
                closeTime: 5000
            });
        } else {
            new FC.MessageLoader($('#result-info'), {
                msgClassName: 'alert-error',
                msg: '系统出错，' + data.msg,
                keepOnly: true
            });
        }
        window.location.href = window.location.href.replace(/#.*/gi, '') + '#top';
    }).fail(function(jqXHR, textStatus) {
        if ($('#task-detail-modal').length > 0) {
            $('#task-detail-modal').modal('hide');
        }
        new FC.MessageLoader($('#result-info'), {
            msgClassName: 'alert-error',
            msg: '系统出错,' + jqXHR.responseText,
            keepOnly: true
        });
        window.location.href = window.location.href.replace(/#.*/gi, '') + '#top';
    });
};

var checkTaskName = function() {
    var basic = $('#basic');
    var databaseSrc = basic.find('[name=database-src]');
    var tableName = basic.find('[name=table-name]');
    var tableNameVal = tableName.val();
    var tableNames = tableNameVal.split(';');
    var taskNameVal = databaseSrc.val() + "##" + tableNames[0];
    taskNameVal = taskNameVal.toLowerCase();
    var taskName = basic.find('[name=task-name]');
    var tableNameControlGroup = tableName.parents('.control-group');
    var taskNameControlGroup = taskName.parents('.control-group');
    basic.find('[name=task-name]').val(taskNameVal);
    tableName.next().remove();
    taskName.next().remove();
    tableNameControlGroup.removeClass('success').removeClass('error');
    taskNameControlGroup.removeClass('success').removeClass('error');
    if (tableNameVal == '') {
        tableNameControlGroup.addClass('error');
        tableName.parent().append($('<span>').attr('class', 'help-inline error').text('请填写结果表名'));
    } else if (tableNameVal.indexOf('.') == -1) {
        tableNameControlGroup.addClass('error');
        tableName.parent().append($('<span>').attr('class', 'help-inline error').text('格式应为数据库.表或模式.表'));
    } else {
        $.ajax({
            url:'/darkbat/json/checkTaskNameAction',
            type: 'post',
            dataType: 'json',
            data: {
                taskName: taskNameVal,
                tableName:tableNameVal
            }
        }).done(function(data) {
            if (data.code == 200) {
                if (data.msg.isExist == 0) {
                    taskNameControlGroup.addClass('success');
                    taskName.parent().append($('<span>').attr('class', 'help-inline').text('任务名称可以使用'));
                } else if (data.msg.isExist == 1) {
                    taskNameControlGroup.addClass('error');
                    taskName.parent().append($('<span>').attr('class', 'help-inline').text('任务名称重复'));
                    tableNameControlGroup.addClass('error');
                    tableName.parent().append($('<span>').attr('class', 'help-inline').text('数据源+结果表名重复，请更换'));
                }
            } else if(data.code == 201){
                tableNameControlGroup.addClass('error');
                tableName.parent().append($('<span>').attr('class', 'help-inline').text('以下结果表在主数据中不存在：'+data.msg.notExistTable.join(';')));
            } else {
                taskNameControlGroup.removeClass('success').addClass('error');
                taskName.parent().append($('<span>').attr('class', 'help-inline').text('检查请求出错'));
            }
        }).fail(function(jqXHR, textStatus) {
            taskNameControlGroup.removeClass('success').addClass('error');
            taskName.parent().append($('<span>').attr('class', 'help-inline').text('系统出错,' + jqXHR.responseText));
        });
    }
};

var checkFreq = function() {
    var basic = $('#basic');
    var freq = basic.find('[name=freq]');
    var freqControlGroup = freq.parents('.control-group');
    freq.next().remove();
    freqControlGroup.removeClass('error');
    if (freq.val() == '') {
        freqControlGroup.addClass('error');
        freq.parent().append($('<span>').attr('class', 'help-inline').text('请填写执行频率'));
    }
};

var checkRecallCode = function() {
    var basic = $('#basic');
    var ifRecallVal = basic.find('[name=if-recall]:checked').val();
    var recallCode = basic.find('[name=recall-code]');
    var recallCodeControlGroup = recallCode.parents('.control-group');
    recallCode.next().remove();
    recallCodeControlGroup.removeClass('error');
    if (ifRecallVal == '1' && recallCode.val() == '') {
        recallCodeControlGroup.addClass('error');
        recallCode.parent().append($('<span>').attr('class', 'help-inline').text('请填写重新执行code'));
    }
};

var checkWaitCode = function() {
    var basic = $('#basic');
    var ifWaitVal = basic.find('[name=if-wait]:checked').val();
    var waitCode = basic.find('[name=wait-code]');
    var waitCodeControlGroup = waitCode.parents('.control-group');
    waitCode.next().remove();
    waitCodeControlGroup.removeClass('error');
    if (ifWaitVal == '1' && waitCode.val() == '') {
        waitCodeControlGroup.addClass('error');
        waitCode.parent().append($('<span>').attr('class', 'help-inline').text('请填写依赖执行code'));
    }
};

var checkSuccessCode = function() {
    var basic = $('#basic');
    var successCode = basic.find('[name=success-code]');
    var successCodeControlGroup = successCode.parents('.control-group');
    successCode.next().remove();
    successCodeControlGroup.removeClass('error');
    if (successCode.val() == '') {
        successCodeControlGroup.addClass('error');
        successCode.parent().append($('<span>').attr('class', 'help-inline').text('请填写成功code'));
    }
};

var changeIfRecall = function() {
    var basic = $('#basic');
    var ifRecallVal = basic.find('[name=if-recall]:checked').val();
    var recallCode = basic.find('[name=recall-code]');
    recallCode.next().remove();
    var recallCodeControlGroup = recallCode.parents('.control-group');
    recallCodeControlGroup.removeClass('error');
    if (ifRecallVal == '0') {
        recallCode.val('').attr('disabled', 'disabled').removeAttr('placeholder');
    } else if (ifRecallVal == '1') {
        recallCode.val('1').removeAttr('disabled').attr('placeholder', '必填项，用;分隔');
    }
};

var changeIfWait = function() {
    var basic = $('#basic');
    var ifWaitVal = basic.find('[name=if-wait]:checked').val();
    var waitCode = basic.find('[name=wait-code]');
    waitCode.next().remove();
    var waitCodeControlGroup = waitCode.parents('.control-group');
    waitCodeControlGroup.removeClass('error');
    if (ifWaitVal == '0') {
        waitCode.val('').attr('disabled', 'disabled').removeAttr('placeholder');
    } else if (ifWaitVal == '1') {
        waitCode.val('1').removeAttr('disabled').attr('placeholder', '必填项，用;分隔');
    }
};

var changeOffsetType = function() {
    var basic = $('#basic');
    var offsetTypeVal = basic.find('[name=offset-type]').val();
    if (offsetTypeVal == 'offset') {
        var offset = basic.find('[name=offset]');
        var td = offset.parent();
        offset.remove();
        td
            .append($('<select>')
                .attr('class', 'input-big')
                .attr('name', 'offset')
                .append($('<option>').attr('value', 'D0').text('D0'))
                .append($('<option>').attr('value', 'D1').text('D1'))
                .append($('<option>').attr('value', 'D2').text('D2'))
                .append($('<option>').attr('value', 'D3').text('D3'))
                .append($('<option>').attr('value', 'D4').text('D4'))
                .append($('<option>').attr('value', 'D5').text('D5'))
                .append($('<option>').attr('value', 'D6').text('D6'))
                .append($('<option>').attr('value', 'M0').text('M0'))
                .append($('<option>').attr('value', 'M1').text('M1'))
                .append($('<option>').attr('value', 'M2').text('M2'))
                .append($('<option>').attr('value', 'M3').text('M3'))
                .append($('<option>').attr('value', 'M4').text('M4'))
                .append($('<option>').attr('value', 'M5').text('M5'))
                .append($('<option>').attr('value', 'M6').text('M6'))
            );
    } else if (offsetTypeVal == 'appoint') {
        var offset = basic.find('[name=offset]');
        var td = offset.parent();
        offset.remove();
        td
            .append(
                $('<input>')
                    .attr('type', 'text')
                    .attr('name', 'offset')
                );
        offset = basic.find('[name=offset]');
        new FC.DatePicker(offset, offset);
    }
};

var getDolInfo = function(){
    $('#dol-btn').button('loading');
    $('#dol-help').text('');
    $('#dol-help').parents('.control-group').removeClass('error');
//    $('#dol').click();
//    $('#dol').removeClass('collapse');
//    $('#basic').addClass('collapse');
//    $('#basic').removeClass('in');

    $.ajax( {
        url : 'json/getDolInfo',
        type : 'post',
        dataType : 'json',
        data : {
            dolName : $('#dol-input').val().trim(),
            group : isEmptyInput($('#group-input'))?"":$('#group-input').val().trim(),
            product : isEmptyInput($('#product-input'))?"":$('#product-input').val().trim()
        },
        async : true,
        success : function(cb) {
            if(cb.code==200){
                dolButton(cb.msg);
            }
            else if(cb.code==500){
                $('#dol-help').text(cb.msg);
                $('#dol-help').parents('.control-group').addClass('error');
            }
            $('#dol-btn').button('reset');
        },
        error: function(jqXHR, textStatus, errorThrown){
            $('#dol-help').text('输入的dol不存在');
            $('#dol-help').parents('.control-group').addClass('error');
            $('#dol-btn').button('reset');
        }
    });
    
};

var dolButton = function(data){
    //show data
    if(data != null && data.tableName.length > 0){
        var tableNames = [];
        $(data.tableName).each(function(i,e){
            if(e.indexOf('.') == -1){
                tableNames.push('bi.'+e.trim());
            } else {
                tableNames.push(e.trim());
            }
        });
        $('#basic').find('[name=table-name]').val(tableNames.join(';'));
        if($('#dol-input').val().indexOf(".dpdm_") > -1) {
            $('#basic').find('[name=task-group-id]').val(3);
        } else if($('#dol-input').val().indexOf(".dprpt_") > -1) {
            $('#basic').find('[name=task-group-id]').val(4);
        } else if($('#dol-input').val().indexOf(".dpmail_") > -1) {
            $('#basic').find('[name=task-group-id]').val(5);
        } else if($('#dol-input').val().indexOf(".dpdw_") > -1) {
            $('#basic').find('[name=task-group-id]').val(6);
        } else {
            $('#basic').find('[name=task-group-id]').val(2);
        }
        if(isEmptyInput($('#group-input'))){
        	$('#basic').find('[name=para1]').val('sh /data/deploy/canaan/bin/dwexec.sh');
        }else{
        	var group = $('#group-input').val();        	
        	var execCode = "sh /data/deploy/sun/bin/ivy.sh velocity -g ";
        	execCode = execCode.concat(group);
        	if (! isEmptyInput($('#product-input'))){
        		var product = $('#product-input').val();
        		execCode = execCode.concat(" -p ");
        		execCode = execCode.concat(product);
        	}
        		
        	$('#basic').find('[name=para1]').val(execCode);
        }
        
        
        $('#basic').find('[name=para2]').val('-dol '+$('#dol-input').val());
        $('#basic').find('[name=para3]').val('-d ${cal_dt} -tid ${task_id}');
        if($('#dol').attr('status') == 0) {
            checkTaskName();
        }
        $('#advanced-body').html('');
        $(data.parentInfo).each(function(i,e){
            var event = {
                data:e
            };
            addNewRow(event);
        });
//        $('#dol').addClass('collapse');
//        $('#dol').removeClass('in');
//        $('#basic').addClass('in');
//        $('#basic').removeClass('collapse');
        if($('#dol').hasClass('in')){
            $('[href=#dol]').click();
        }
        if(!$('#basic').hasClass('in')){
            $('[href=#basic]').click();
        }
    }
    else{
        $('#dol-help').text('输入的dol没有结果表名');
        $('#dol-help').parents('.control-group').addClass('error');
    }
};
