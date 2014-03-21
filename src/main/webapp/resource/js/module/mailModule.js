define(function (require) {
    var
        baseModule = require('localJs/module/base/baseModule'),
        _mailListMap = {}
    ;

    function _mailToZTreeNode (mails) {
        var nodes = [];
        $(mails).each(function(i, e) {
            nodes.push({
                id: e.mailId,
                pid: -1,
                name: e.mailTitle
            });
        });
        return nodes;
    }

    function _getMailListMap (mails) {
        var map = {};

        $(mails).each(function(i, e) {
            var id = e.mailId.toString();

            if(!map[id]) {
                map[id] = e;
            }
        });

        return map;
    }

    /**
     * 渲染邮件列表
     * @param  {[type]} json    查询条件
     * @param  {[type]} options {
     *                              url:
     *                              treeId:
     *                              onClick: function(event, treeId, treeNode)
     *                              $container
     *                          }
     * @return {[type]}         [description]
     */
    baseModule.renderMailList = function (json, options) {
        baseModule.getList(json, {
            url: options.url,
            onSuccess: function(data) {
                if(data.code === 200) {
                    _mailListMap = _getMailListMap(data.msg);
                    var nodes = _mailToZTreeNode(data.msg);
                    var treeSetting = {
                        treeId: options.treeId,
                        view: {
                            selectedMulti: false
                        },
                        data: {
                            simpleData: {
                                enable: true
                            }
                        },
                        callback: {
                            onClick: options.onClick
                        }
                    };

                    $.fn.zTree
                        .init(options.$container.html(''), treeSetting, nodes)
                        .expandAll(true);
                }
            }

            
        });
    }

    baseModule.getMailsMap = function() {
        return _mailListMap;
    }

    baseModule.addOrUpdateMailInfo = function(json, options) {
        baseModule.doQuery(json, options);
    }

    baseModule.getMailById = function(mailId, options) {
        var id = mailId.toString(),
            mail = _mailListMap[id];

        if(mail) {
            options.onSuccess({
                code: 200,
                msg: [mail]
            });
        } else {
            baseModule.getOne({
                mailId: mailId
            }, options);
        }
    } 

    return baseModule;
});