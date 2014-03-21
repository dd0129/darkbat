define(function(require) {
    require('jquery');
    require('bootstrap');
    require('halleyPrototype');
    require('halleyCommon');
    require('zTree');

    var comm        = require('localJs/common/common'),
        mailModule  = require('localJs/module/mailModule'),
        chooseModalUtil = require('localJs/common/util/chooseModal'),
        resourceUtil = require('localJs/common/util/resource'),
        Global      = require('halleyGlobal'),
        mailEvents  = require('localJs/events/mail.config.events'),
        modalUtil   = require('localJs/common/util/modalWithId'),
        reportEditModal = require('localJs/common/util/reportEditModal'),
        pageEditModal = require('localJs/common/util/pageEditModal');

    var _rptToNodeMap;

    userVerify = new Global.UserVerify();
    userVerify.verifyLogin(function() {
        mailModule.init(userVerify.token);
        
        createBreadcrumb();
        renderMailList();
        bindAddMail();
        bindCheckExist();
        bindChooseSendCycle();
    });

    //创建面包屑
    function createBreadcrumb() {
        new FC.Breadcrumb($('#bread-crumb'), {
            siteStruction: {
                secondLevel: {
                    title: '北斗平台',
                    subSites: [{
                        title: '报表配置',
                        url: comm.isOnline ? 'http://venus.data.dp/report-designer.html' : '/venus/report-designer.html'
                        }, {
                            title: '报表查看',
                            url: comm.isOnline ? 'http://venus.data.dp/index.html' : '/venus/index.html'
                        }, {
                            title: '邮件配置',
                            url: 'mail-config.html'
                        }
                    ]
                }
            },
            subSiteIndex: 2
        });
    }
    
    function bindAddMail() {
        var $btn = $('#mail-add');
        $btn.click({
            buildChooseReportBtn:   buildChooseReportBtn,
            mailEvents:             mailEvents,
            renderMailList:         renderMailList,
        }, mailEvents.clickAddMail);

        $btn.click();
    }

    function renderMailList() {
        mailModule.renderMailList(null, {
            url: 'json/getMailList',
            treeId: 'mail-list',
            $container: $('#mail-tree').html(''),
            onClick: function(event, treeId, treeNode) {
                // 点击左侧树, 展开详情
                var id = treeNode.id;
                mailModule.getMailById(id, {
                    onSuccess: function(data) {
                        if(data.code === 200) {
                            var html = FC.getHtmlTemplate('template/mail-info.html', data.msg[0], {
                                    transformFoos: {
                                        sendCycleTransform: function($ele) {
                                            var val = $ele.text();
                                            val =
                                                val === 'D' ? '日' :
                                                val === 'W' ? '周' :
                                                val === 'M' ? '月' : 'UNKNOWN';

                                            $ele.text(val);
                                        },
                                        timeRangeTransform: function($ele, json) {
                                            if(json.sendCycle !== 'D') {
                                                $ele.remove();
                                            } else {
                                                var $label = $ele.find('.controls>label'),
                                                    val = $label.text();

                                                if(val) {
                                                    $label.text(val + '日内');
                                                } else {
                                                    $label.text('未设置');
                                                }
                                            }
                                        },
                                        itemTypeTransform: function ($ele, json) {
                                            var str = json.itemType === 'REPORT' ? '报表' : 'Dashboard';
                                            $ele.text(str);
                                        },
                                        mailContentTransform: function($ele, json) {
                                            if(!json.mailContent) {
                                                $ele.replaceWith('<label class="padding-label">未设置</label>');
                                            }
                                        },
                                        idToRptNameTransform: function($ele, json) {
                                            var ids = json.itemIdList;
                                            $.ajax({
                                                url: 'json/getMailItems',
                                                data: {
                                                    reportIds: ids,
                                                    mailId: json.mailId,
                                                },
                                                async: false,
                                                type: 'POST',
                                                dataType: 'json'
                                            }).done(function(cb) {
                                                if(cb && cb.code === 200) {
                                                    var initHtmls = '', itemType = json.itemType;

                                                    if (itemType === 'REPORT') {
                                                        initHtmls =
                                                            '<table class="table-bordered table" item-type="' + itemType + '" style="width:360px;">' +
                                                                '<thead>' +
                                                                    '<tr>' +
                                                                        '<th>报表名称</th>' +
                                                                        '<th>数据周期</th>' +
                                                                    '</tr>' +
                                                                '</thead>' +
                                                                '<tbody>';
                                                        $(cb.msg).each(function(i, e) {
                                                            initHtmls +=
                                                                '<tr>' +
                                                                    '<td class="item-name" item-id=' + e.reportId + '>' + e.itemName + '</td>' +
                                                                    '<td>' + e.dataCycle + '</td>' +
                                                                '</tr>';
                                                        });
                                                    } else if (itemType === 'PAGE') {
                                                        cb.msg.sort(function(a, b) {
                                                            return (a.displayIndex - b.displayIndex);
                                                        });
                                                        initHtmls =
                                                            '<table class="table-bordered table" item-type="' + itemType + '" style="width:360px;">' +
                                                                '<thead>' +
                                                                    '<tr>' +
                                                                        '<th>名称</th>' +
                                                                        '<th>类型</th>' +
                                                                        '<th>显示选项</th>' +
                                                                        '<th>数据周期</th>' +
                                                                    '</tr>' +
                                                                '</thead>' +
                                                                '<tbody>';

                                                        $(cb.msg).each(function(i, e) {
                                                            initHtmls +=
                                                                '<tr>' +
                                                                    '<td class="item-name" item-id=' + e.reportId + '>' + e.itemName + '</td>' +
                                                                    '<td>' + (e.itemType === 'TABLE' ? '报表' : '图表') + '</td>' +
                                                                    '<td>' + (e.isHide ? '隐藏' : '显示') + '</td>' +
                                                                    '<td>' + (e.dataCycle ? e.dataCycle : 'MTD') + '</td>' +
                                                                '</tr>';
                                                        });
                                                    }
                                                    initHtmls += '</tbody></table>';
                                                    $ele.html(initHtmls);
                                                } else {
                                                    $ele.html('<p style="color: red;">获取报表信息出错, 出错信息: ' + cb.msg + '</p>');
                                                }

                                            }).fail(function(cb) {
                                                $ele.html('<p style="color: red;">获取报表信息出错, 出错信息: ' + cb.responseText + '</p>');
                                            });
                                        },
                                        emailUserListTransform: function($ele, json) {
                                            var emails = json.userEmailList.split(',');

                                            for(var i = 0; i < emails.length; i++) {
                                                var str = emails[i];
                                                
                                                if(i < emails.length - 1) {
                                                    str += ', ';
                                                  //每4个名字换行一次
                                                    if(i > 0 && i%4 === 0) {
                                                        str += '<br/>';
                                                    }
                                                }

                                                $ele.append(str);
                                            }
                                        }
                                    }
                                }),
                                $div = $(html);
                                $div.find('#mail-delete').click({
                                    mailId:                 data.msg[0].mailId,
                                    taskId:                 data.msg[0].taskId,
                                    renderMailList:         renderMailList,
                                    mailTitle:              data.msg[0].mailTitle,
                                    
                                }, mailEvents.deleteMailInfo);

                                $div.find('#mail-send').click({
                                    taskId: data.msg[0].taskId,
                                    mailTitle: data.msg[0].mailTitle
                                }, mailEvents.resendMail);

                                $div.find('#mail-edit').click({
                                    buildChooseReportBtn:   buildChooseReportBtn,
                                    mailEvents:             mailEvents,
                                    renderMailList:         renderMailList,
                                    mailId:                 data.msg[0].mailId,
                                    taskId:                 data.msg[0].taskId,
                                    reportIds:              data.msg[0].itemIdList,
                                }, mailEvents.editMailConfig);

                            $('#mail-designer-container').empty().append($div);
                        }
                    }
                });
            }
        });
    }

    function bindCheckExist() {
        var $input = $('#mail-title input');
        $input.blur({
            mailModule: mailModule
        }, mailEvents.checkIsExist);
    }

    function bindChooseSendCycle() {
        $('#sendCycle').find('input[type=radio]').click(mailEvents.onChooseSendCycle);
    }

    /**
     * 点击添加报表
     * @return {[type]} [description]
     */
    function buildChooseReportBtn(itemIds, mailId) {
        var modalId = 'report-choose-modal';
        comm.clearModal(modalId);

        chooseModalUtil.initControl({
            id:         'report-chooser',
            name:       '报表/Dashboard',
            placeholder: '点击选择...',
            btnName:    '点击选择',
            dataKey:    'itemIdList',
            $container: $('#report-chooser-container'),
            inputClass: 'input-xlarge',
            initController: function($controller) {
//                if(reportIds) {
//                    var $input0 = $controller.find('.input-first'),
//                        $input1 = $controller.find('.input-second');
//
//                    $input0.val(reportIds.join(', '));
//                    $input1.val(reportIds.join('<+>'));
//                    
//                }
            }
        }, {
            message: '选择报表',
            innerHTML: '' +
                '<ul class="nav nav-tabs">' +
                    '<li class="active"><a href="#report-list" data-toggle="tab">选择报表</a></li>' +
                    '<li><a href="#page-list" data-toggle="tab">选择Dashboard</a></li>' +
                '</ul>' +
                '<div class="tab-content">' +
                    '<div class="tab-pane active" id="report-list">' +
                        '<ul class="ztree" id="tree-report"></ul>' +
                        '<input type="text" class="hide report-ids">' +
                        '<input type="text" class="hide report-names">' +
                    '</div>' +
                    '<div class="tab-pane" id="page-list">' +
                        '<ul class="ztree" id="tree-page"></ul>' +
                        '<input type="text" class="hide report-ids">' +
                        '<input type="text" class="hide report-names">' +
                    '</div>' +
                '</div>',
            initModal: function($pDiv) {
                seajs.use('localJs/module/base/baseModule', function(baseModule) {
                    var treeSetting = {
                        treeId: "unkown",
                        view: {
                            selectedMulti: true
                        },
                        data: {
                            simpleData: {
                                enable: true
                            }
                        },
                        callback: {
                            onCheck: mailEvents.checkReportNode
                        },
                        check: {
                            enable: true,
                            autoCheckTrigger: true,
                            chkStyle: 'radio',
                            radioType: 'all'
                        }
                    };
                    var listCallBack = function(resourceType, $container, data) {
                        var nodes = resourceUtil.resourcesToZTreeNodes(data.msg, resourceType);
                        nodes.splice(0, 1);
                        var mySetting = FC.cloneObject(treeSetting);
                        var isPage = resourceType === 'PAGE';
                        
                        var editHandler = isPage ? pageEditModal : reportEditModal;
                        mySetting.check.chkStyle = isPage ? 'radio' : 'checkbox' ;
                        mySetting.treeId = resourceType || 'UNKNOWN';

                        var zTreeObj = $.fn.zTree
                            .init($container.html(''), mySetting, nodes);
                        zTreeObj.expandAll(false);
                        
                        _itemToNodeMap =  mailEvents.getRptToNodeMap(zTreeObj.transformToArray(zTreeObj.getNodes()));

                        $(zTreeObj.getNodesByFilter(function(node) {
                            return node.isParent;
                        })).each(function(i, e) {
                             $('#' + e.tId).find('span:eq(1)').addClass('hide');
                        });

                        editHandler.setOptions({nodes:_itemToNodeMap});
                        if (itemIds) {
                            $(itemIds).each(function(i, e) {
                                var node = _itemToNodeMap[e.toString()];
                                $('#' + node.tId).find('span.chk').click();
                            });
                        }

                        $('#mail-confirm').removeClass('disabled');
                        $('#report-chooser a.btn').removeClass('disabled');
                    };
                    baseModule.getList(null, {
                        url: 'json/getPageListFromVenus',
                        async: false,
                        onSuccess: function(data) {
                            if (data && data.code === 200) {
                                listCallBack(resourceUtil.resourceType.PAGE, $('#page-list>ul'), data);
                            }
                        }
                    });
                    baseModule.getList(null, {
                        url: 'json/getReportListFromVenus',
                        async: false,
                        onSuccess: function(data) {
                            if (data.code === 200) {
                                listCallBack(resourceUtil.resourceType.REPORT, $('#report-list>ul'), data);
                            }
                        }
                    });
                });
            },
            onSubmit: mailEvents.submitReportChooserModal,
            isRefresh: false,
            modalId: modalId
        });
    }
});
