define(function(require) {
    require('jquery');
    
    var _comm       = require('localJs/common/common'),
        _mailModule = require('localJs/module/mailModule'),
        _modalUtil  = require('localJs/common/util/modalWithId'),
        reportEditModal = require('localJs/common/util/reportEditModal'),
        pageEditModal = require('localJs/common/util/pageEditModal');

    var globalMailId = null;
    /**
     * 点击添加邮件报表生成邮件单
     * @param  {[type]} e       [description]
     * @param  {[type]} options {
     *                              reportIds: [],
     *                              emails: []   
     *                          }
     * @param  {[type]} options {
     *                              formName: str
     *                          }
     * @return {[type]}   [description]
     */
    function _getMailForm(e, options, formOptions) {
        var html                    = FC.getHtmlTemplate('template/mail-form.html', formOptions),
            $div                    = $(html),
            buildChooseReportBtn    = e.data.buildChooseReportBtn,
            mailEvents              = e.data.mailEvents,
            renderMailList          = e.data.renderMailList,
            ailList;

        $div.attr('data-type', 'add');
        $div.appendTo($('#mail-designer-container').empty());
        
        _bindAutoHeight('#user-mail-list');
        _bindAutoHeight('#mail-content');
        
        options = options || {};
        buildChooseReportBtn(options.reportIds, formOptions.mailId);

        $div.find('#mail-confirm').click({
            renderMailList: renderMailList,
            type:           $div.attr('data-type'),
            mailId: formOptions.mailId || '',
            taskId: formOptions.taskId || ''
        }, mailEvents.clickMailConfirm);
    }

    $.fn.autosize = function() {
        if ($(this).attr('id')) {
            $(this).height('0px');
            var setheight = document.getElementById($(this).attr('id')).scrollHeight;
            setheight = setheight > 100 ? setheight : 100;
            $(this).height(setheight + 'px');
        }
    };
    
    //textarea自动调整高度
    function _bindAutoHeight(id) {
        $(id).on('keydown keyup', function() {
            $(this).autosize();
        }).show().autosize();
        $(id).attr('style',$(id).attr('style') + 'overflow-y:hidden;');
    }
    
    function _showMsg(options, cssOptions) {
        var $div = _modalUtil.showMessage(options);

        if(cssOptions) {
            _modalUtil.fixCss($div, cssOptions);
        }
        
        return $div;
    }

    function sendTestMail(e) {
        var $me = $(this);

        var actionStr =
            'json/sendTestMail?taskId=' + e.data.taskId + '&recipient=' + e.data.recipient;

        $.ajax({
            url: actionStr,
            type: 'GET',
            dataType: 'json'
        }).done(function(cb) {
            if(cb.code && cb.code === 200) {
                _showMsg({
                    message: '操作成功',
                    innerHTML: '' +
                        '<div class="alert alert-success">测试邮件任务已经启动</div>',
                    hasSubmit: false
                });
            } else {
                _showMsg({
                    message: '操作失败',
                    innerHTML: '' +
                        '<div class="alert alert-error">' +
                            '测试邮件任务启动失败, 错误信息: <br/>' + cb.msg + ' (可能由于报表Sql中存在单选控件使用=的情况，查明后用in取代=) ' +
                        '</div>',
                    hasSubmit: false
                });
            }
        }).fail(function(cb) {
            _showMsg({
                message: '操作失败',
                innerHTML: '' +
                    '<div class="alert alert-error">' +
                        '测试邮件任务启动失败, 错误信息: <br/>' + cb +
                    '</div>',
                hasSubmit: false
            });
        });
        e.stopPropagation();
    }

    //检测接收邮箱字符串是否有效
    function _validateUserMailList(mails) {
        var text = mails.replace(/\s/gi,'').replace(/，/gi,',');
        var reg = /^[\w|\-|,|\.|@]+$/gi;
        if(text.length === 0 || !reg.test(text)){
            return false;
        }
        return true;
    }
    
    return {
        deleteMailInfo: function(e) {
            var $me = $(this);

            new FC.ConfirmBox($me, {
                msg: '确定删除此邮件报表？',
                doConfirm: function(arg) {
                    _mailModule.deleteOne({
                        mailId: arg.mailId,
                        taskId: arg.taskId,
                        renderMailList: arg.renderMailList
                    }, {
                        url: 'json/deleteMailInfo',
                        onSuccess: function(e) {
                            _showMsg({
                                message: '删除成功',
                                innerHTML: '' +
                                    '<div class="alert alert-success">' +
                                        '<a class="close" data-dismiss="alert">×</a>' +
                                        '邮件<strong>' + arg.mailTitle + '</strong>删除成功!' +
                                    '</div>',
                                hasSubmit: true,
                                onSubmit: function($div) {
                                    _modalUtil.removeModal($div);
                                    $('#mail-add').click();
                                },
                                submitText: '确定',
                                hasClose: false
                            });
                            arg.renderMailList();
                        }
                    });
                }, eventData: e.data
            });
            e.stopPropagation();
        },

        resendMail: function(e) {
            var $me = $(this);
                
            var today = new Date(),
                todayStr = today.format('yyyy-MM-dd');
                
            today.setDate(today.getDate() + 1);
            var tomorrowStr = today.format('yyyy-MM-dd');

            var actionStr =
                'json/resendMail?taskId=' + e.data.taskId + '&begin=' + todayStr + '&end=' + tomorrowStr;

            new FC.ConfirmBox($me, {
                msg: '将发送邮件<strong>' + e.data.mailTitle + '</strong>, 确定执行?',
                eventData: actionStr,
                doConfirm: function(arg) {
                    $.ajax({
                        url: actionStr,
                        type: 'GET',
                        dataType: 'json'
                    }).done(function(cb) {
                        if(cb.code && cb.code === 200) {
                            _showMsg({
                                message: '操作成功',
                                innerHTML: '' +
                                    '<div class="alert alert-success">邮件任务已经启动，开始重新发送</div>',
                                hasSubmit: false
                            });
                        } else {
                            _showMsg({
                                message: '操作失败',
                                innerHTML: '' +
                                    '<div class="alert alert-error">' +
                                        '邮件任务启动失败, 错误信息: <br/>' + cb.msg +
                                    '</div>',
                                hasSubmit: false
                            });
                        }
                    }).fail(function(cb) {
                        _showMsg({
                            message: '操作失败',
                            innerHTML: '' +
                                '<div class="alert alert-error">' +
                                    '邮件任务启动失败, 错误信息: <br/>' + cb +
                                '</div>',
                            hasSubmit: false
                        });
                    });
                }
            });
            e.stopPropagation();
        },
        
        editMailConfig: function(e) {
            $('#report-choose-modal').remove();

            globalMailId = e.data.mailId;
            var mailId = e.data.mailId;
            var taskId = e.data.taskId;

            _mailModule.getMailById(mailId, {
                onSuccess: function(data) {
                        
                    if(data.code === 200) {

                        var mail = data.msg[0];

                        _getMailForm(e, {
                            reportIds: mail.itemIdList.split(',')
                        }, {
                            formName: '<strong>' + mail.mailTitle + '</strong>邮件配置',
                            mailId: mailId,
                            taskId: taskId
                        });
                        
                        var $pDiv = $('#wizard');

                        $pDiv.attr('data-type', 'edit').attr('data-mailId', mail.mailId);
                       
                        $pDiv.find('[data-key="mailTitle"]').val(mail.mailTitle);
                        $pDiv.find('[data-key="mailContent"]').val(mail.mailContent);
                        $pDiv.find('[data-key="sendCycle"]').filter('[value=' + mail.sendCycle + ']').click();
                        $pDiv.find('[data-key="sendTime"]').val(mail.sendTime);
                        $('#user-mail-list').val(mail.userEmailList);
                        
                        if(mail.sendCycle) {
                            $pDiv.find('select[data-key="timeRange"]').children().filter('[value=' + mail.timeRange + ']').attr('selected', true);
                        }
                    }
                }
            });
        },
        
        clickMailConfirm: function(e) {
            var $me = $(this);
            if($me.attr('disabled')) {
                return;
            }
            
            $('.control-group').removeClass('error');
            
            var $mailsTextInput = $('#user-mail-list'),
                mails = $mailsTextInput.val();
            if(!_validateUserMailList(mails)) {
                $mailsTextInput.parents('.control-group').addClass('error');
                return;
            } else {
                var mailList = mails.replace(/\s/g, '').replace(/，/gi,',').split(',');
                var nameList = [];
                for(var i = 0; i < mailList.length; i++){
                    nameList.push(mailList[i].split('@')[0]);
                }
                $('#email-chooser-container').find('[data-key="userEmailList"]').val(nameList.join('<+>'));
            }
            
            var $div = $('#add-query-form'),
                $inputs = $div.find('[required]').filter(':not(.ignore)'),
                $undo = FC.getUnputInputs($inputs),
                type = e.data.type;

            if($undo.length) {
                for(var i = 0; i < $undo.length; i++) {
                    $undo[i].parents('.control-group').addClass('error');
                }
                return;
            }
            
            var json = FC.initFormJson($div),
                sendTime = json.sendTime.trim().replace(/：/i, ':'),
                regHhmm = /^\d{2}\:\d{2}$/,
                systemId = FC.getQueryStringByName('systemId'),
                mailId = e.data.mailId,
                taskId = e.data.taskId;

            if(!regHhmm.test(sendTime)) {
                $('#send-time').addClass('error');

                var modalId = 'info-modal';
                _showMsg({
                    message: '输入有误',
                    innerHTML: '' +
                        '<div class="alert alert-error">' +
                            '发送时间时间格式应为 <em>hh:mm</em>' +
                        '</div>',
                    hasClose: false,
                    submitText: '确定',
                    onSubmit: function() {
                        _comm.clearModal(modalId);
                    },
                    modalId: modalId
                });

                return;
            }

            $inputs.each(function(i, e) {
                $(e).parents('.control-group').removeClass('error');
            });

            systemId = systemId ? systemId : 0;
            json.mailId = mailId;
            json.taskId = taskId;
            json.systemId = systemId;
            json.addUser = userVerify.getPinyinName();
            json.updateUser = userVerify.getPinyinName();

            var $reportCycleEdit = $('#report-chooser-edit'),
                $pageCycleEdit = $('#page-item-chooser-edit'),
                dataCycle = null, itemType = null;

            if ($reportCycleEdit.hasClass('hide')) {
                //选择为图表的配置
                dataCycle = pageEditModal.getPageItemEdit();
                itemType = 'PAGE';
                                
                json.itemIdList = $pageCycleEdit.find('tbody').attr('page-id');
            } else {
                //选择为报表的配置
                dataCycle = reportEditModal.getReportEdit();
                itemType = 'REPORT';
                
                var itemIdList = [];
                $(dataCycle).each(function(i,e){
                    itemIdList.push(e.id);
                });
                json.itemIdList = itemIdList.join(",");
            }
            json.itemType = itemType;

            json.mailDetail = FC.jsonToString(dataCycle);
            if(dataCycle.length === 0){
                $('#report-chooser-container a').addClass('btn-danger');
                $('#report-chooser-container a').removeClass('btn-primary');
                return;
            }
            else{
                $('#report-chooser-container a').addClass('btn-primary');
                $('#report-chooser-container a').removeClass('btn-danger');
            }
            $me.button('loading');

            _mailModule.addOrUpdateMailInfo(json, {
                url: 'json/addOrUpdateMailInfo',
                onSuccess: function(cb) {
                    $me.button('reset');
                    var modalId = '';
                    
                    if(cb.code === 200) {
                        modalId = 'success-info';
                        var $div = _showMsg({
                            message: '操作成功',
                            innerHTML: '' +
                                '<div class="alert alert-success">' +
                                    '邮件<strong>' + json.mailTitle + '</strong>操作成功！' +
                                    '<a class="btn sendTestMail">发送测试邮件</a>' +
                                '</div>',
                            hasClose: false,
                            hasSubmit: true,
                            submitText: '确定',
                            onSubmit: function($div) {
                                e.data.renderMailList();
                                _modalUtil.removeModal($div);
                                $('#mail-add').click();
                            },
                            modalId: modalId
                        });
                        
                        $div.find('.sendTestMail').click({
                            taskId: json.taskId == '' ? cb.msg.taskId : json.taskId,
                            recipient: json.updateUser
                        }, sendTestMail);
                    } else {
                        modalId = 'error-info';
                        _showMsg({
                            message: '操作失败',
                            innerHTML: '' +
                                '<div class="alert alert-error">' +
                                    '邮件<strong>' + json.mailTitle + '</strong>操作失败!<br/>' +
                                    '错误信息: ' + cb.msg +
                                '</div>',
                            hasClose: false,
                            hasSubmit: true,
                            submitText: '确定',
                            onSubmit: function() {
                                _comm.clearModal(modalId);
                            },
                            modalId: modalId
                        });
                    }
                }
            });
        },

        clickAddMail: function(e) {
            globalMailId = null;
            _getMailForm(e, null, {
                formName: '邮件配置'
            });
        },

        checkReportNode: function(event, treeId, treeNode) {
            treeNode =
                treeNode    ? treeNode : 
                treeId      ? treeId   : event;

            if(!treeNode.isParent) {
//                var $div = $('#report-list'),
//                    rptId = treeNode.resourceId.toString();
//
//                var $inputId = $div.find('.report-ids'),
//                    $inputName = $div.find('.report-names');
//
//                var ids = $inputId.val() || '',
//                    names = $inputName.val() || '';
//
//                var idsArr = ids.split('<+>'),
//                    namesArr = names.split(', ');

                if(treeNode.checked) {
//                    var id = rptId;
//
//                    if(!_comm.hasElementInArray(idsArr, id)) {
//                        idsArr.push(id);
//                        namesArr.push(treeNode.name);
//                    }
//                    
                    var isPage = treeNode.type === 'PAGE';
                    var url = treeNode.type === 'PAGE' ? 'json/getPageItemListFromVenusByIds' : 'json/getReportListFromVenusByIds';
                    

                    var data = {
                        mailId: globalMailId || ''
                    };
                    var key = isPage ? 'pageId' : 'reportIds';
                    data[key] = treeNode.resourceId;

                    $.ajax({
                        url: url,
                        data: data,
                        async: false,
                        type: 'POST',
                        dataType: 'json'
                    }).done(function(cb){
                        if (isPage) {
                            pageEditModal.addPageItemEdit(cb.msg);
                        } else {
                            reportEditModal.addReportEdit(cb.msg[0]);
                        }
                    });
                } else {
//                    _comm.deleteOneEleFromArr(idsArr, rptId);
//                    _comm.deleteOneEleFromArr(namesArr, treeNode.name);
                    if (isPage) {
                        pageEditModal.delPageItemEdit({
                            itemId: treeNode.resourceId
                        });
                    } else {
                        reportEditModal.delReportEdit({reportId:treeNode.resourceId});
                    }
                }

//                var reg0 = /(^\<\+\>)/,
//                    reg1 = /(^\,)/;
//                
//                $inputId.val(idsArr.join('<+>').replace(reg0, '').trim());
//                $inputName.val(namesArr.join(', ').replace(reg1, '').trim());
            }
        },

        /**
         * 提交报表选择器
         * @param  {[type]} $pDiv [description]
         * @return {[type]}       [description]
         */
        submitReportChooserModal: function($pDiv) {
            var $activeDiv = $pDiv.find('.tab-content .active'),
                $rEditDiv = $('#report-chooser-edit').addClass('hide'),
                $pEditDiv = $('#page-item-edit').addClass('hide');

            if ($activeDiv.attr('id') === 'report-list') {
                $rEditDiv.removeClass('hide');
            } else {
                $pEditDiv.removeClass('hide');
            }
            // var ids = $pDiv.find('.report-ids').val();
            // var names = $pDiv.find('.report-names').val();

            // var $chooser = $('#report-chooser');
            // $chooser.find('.input-first').val(names);
            // $chooser.find('.input-second').val(ids);

            return true;
        },

        /**
         * 邮件输入器删除一行数据
         * @param  {[type]} e [description]
         * @return {[type]}   [description]
         */
        clickAddRow: function(e) {
            var $table = e.data.$table,
                $trs = $('' +
                    '<tr>' +
                        '<td><input type="text" class="mail-table-input">@dianping.com</td>' +
                        '<td><button class="btn btn-delete"><i class="icon-remove"></i></button></td>' +
                    '</tr>');
            
            $trs.find('.btn-delete').on({
                'click': function() {
                    $(this).parents('tr').remove();
                }
            });
            $trs.appendTo($table.find('tbody'));
        },

        submitEmailInputerModal: function($pDiv) {
            var $inputs = $pDiv.find('input'),
                ret = [];

            $inputs.each(function(i, e) {
                var val = $(e).val().trim();
                if(val !== '') {
                    ret.push(val);
                }
            });

            var $chooser = $('#email-chooser');
            if(ret.length) {
                
                $chooser.find('.input-first').val(ret.join(', '));
                $chooser.find('.input-second').val(ret.join('<+>'));

                return true;
            } else {
                new FC.MessageLoader($chooser.find('.back-info'), {
                    msgClassName: 'alert-error',
                    msg: '请输入接收人邮箱',
                    keepOnly: true
                });
                return false;
            }
        },

        checkIsExist: function(e) {
            var mailTitle  = $(this).val().trim(),
                mailId = $('#wizard').attr('data-id');

            if(mailTitle === '') {
                return;
            }
            _mailModule.getList({
                mailTitle: mailTitle,
                mailId: mailId || ''
            }, {
                url: 'json/getExistMailInfo',
                onSuccess: function(data) {
                    if(data.code === 200) {
                        if (data.msg.length !== 0) {
                            $('#mail-title').addClass('error');

                            new FC.MessageLoader($('#mail-config-back-info'), {
                                msgClassName: 'alert-error',
                                msg: '邮件名称已经存在, 请更换一个名称!',
                                keepOnly: true,
                                closeTime: 10000
                            });
                        }
                    }
                }
            });
        },

        onChooseSendCycle: function(e) {
            var $me = $(this),
                val = $me.val(),
                $div = $('#time-range'),
                $select = $div.find('select');

            if(val === 'D') {
                $div.removeClass('hide');
                $select.removeClass('ignore');
            } else {
                $div.addClass('hide');
                $select.addClass('ignore');
            }
        },

        /**
         * 构造表id -> 节点的字典信息
         * @param  {[type]} nodes [description]
         * @return {[type]}       [description]
         */
        getRptToNodeMap: function(nodes) {
            var ret = {};
            $(nodes).each(function(i, e) {
                if(!e.isParent) {
                    ret[e.resourceId.toString()] = e;
                }
            });

            return ret;
        }
    };
});
