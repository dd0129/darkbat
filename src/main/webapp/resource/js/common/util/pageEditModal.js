define(function(require) {
    require('jquery');
    require('halleyCommon');

    var _DATE_CYCLE_MTD = 0;

    var _getPageDetail = function(pageInfo) {
        var dashletCfgObj = pageInfo.dashletConfig;
        var paramCfgObj = pageInfo.paramConfig;

        var paramMap = {};
        $(paramCfgObj).each(function(i, region) {
            $(region).each(function(i, e) {
                paramMap[e.paramId.toString()] = e;
            });
        });

        var allItems = [];
        $(dashletCfgObj).each(function(i, region) {
            var items = [];
            $(region).each(function(i, e) {
                var paramDetails = [];

                $(e.params).each(function(i, e) {
                    paramDetails.push(paramMap[e.toString()]);
                });
                e.paramDetails = paramDetails;
                items.push(e);
            });
            allItems = allItems.concat(items);
        });
        pageInfo.items = allItems;
            
        return pageInfo;
    };

    var nodes = null, _tableId = 'mail-detail-cfg',

        _selectHtml = (function(){
            var html =
                '<select class="page-item-data-cycle input-small" page-item-data-cycle>' +
                    '<option value="0">MTD</option>';
            for (var i = 1; i <= 31; i++) {
                html += '<option value="' + i + '">' + i + '</option>';
            }

            return html + '</select>';
        })();

    function _initPageItemEdit(data) {
        $div = $('#page-item-chooser-edit');
        var initHtml =
            '<div class="control-group row-fluid" style="margin-left: 160px;">' +
                '<div class="controls span8">' +
                    '<table class="table-bordered table" style="width: 500px;" id="' + _tableId + '">' +
                        '<thead><tr>' +
                            '<th width="37%">名称</th>' +
                            '<th width="10%">类型</th>' +
                            '<th width="14%">时间周期</th>' +
                        '</tr></thead>' +
                        '<tbody></tbody>' +
                    '</table>' +
                '</div>' +
            '</div>';
        $div.html(initHtml);

        $(data).each(function(i, e) {
            _addPageItemEdit(e);
        });
    }

    function _typeTranslator(type) {
        switch (type) {
            case 'chart': return '图表';
            case 'table': return '报表';
            default: return '设置错误';
        }
    }
    function _addPageItemEdit(data) {
        var pageInfo = _getPageDetail(data);

        $('#report-chooser-edit').addClass('hide');
        var $pDiv = $('#page-item-chooser-edit'), $div = $pDiv.find('tbody');
        if ($div.length) {
            var hides = [];
            $div.html('').attr('page-id', data.pageId);
            
            $(pageInfo.items).each(function(idx, e) {
                var canEdit = false;
                for (var i = 0; i < e.paramDetails.length; i++) {
                    var param = e.paramDetails[i];
                    if (
                        param.paramType === 'CASCADE' &&
                        (param.paramSubtype === 'DATE' || param.paramSubtype === 'MONTH')
                    ) {
                        canEdit = true;
                        break;
                    }
                }

                 var html = '' +
                        '<tr>' +
                            '<td class="page-item-name" page-item-id="' + e.id + '">' + e.name + '</td>' +
                            '<td class="page-item-type" page-item-type="' + e.type + '">' + _typeTranslator(e.type) + '</td>' +
                            '<td>' + _selectHtml + '</td>' +
                        '</tr>';
                var $tr = $(html);
                $div.append($tr);

                if (e.isEdit) {
                    $tr.find('select').val(e.dataCycle);

                    if (e.isHide)  {
                        hides.push(idx);
                    }
                }
            });

            seajs.use('localJs/common/util/sortableTable.util', function(util) {
                util.init({
                    tableId: '#' + _tableId,
                    hasDelete: false,
                    hasSort: true,
                    hasHide: true,
                    hides: hides
                });
            });
        } else {
            _initPageItemEdit([ data ]);
        }

        $pDiv.removeClass('hide');
    }

    function _disappearPageItemEdit() {
        var data = _getPageItemEdit();
        if (data.length === 0) {
            $('#page-item-chooser-edit').html('');
        }
    }

    function _getPageItemEdit() {
        var data = [];
        $('#page-item-chooser-edit tbody tr').each(
                function(i, e) {
                    var $e = $(e), isHide = $e.attr('isHide');
                    data.push({
                        id : $e.find('.page-item-name').attr('page-item-id'),
                        name : $e.find('.page-item-name').text(),
                        type: $e.find('.page-item-type').attr('page-item-type'),
                        cycle : $e.find('.page-item-data-cycle').val(),
                        displayIndex: $e.data('display-index') || (i + 1),
                        isHide: isHide ? (isHide === 'true' ? 1 : 0) : 0
                    });
                });
        return data;
    }

    return {
        setOptions : function(options) {
            if (options.nodes)
                nodes = options.nodes;
        },
        initPageItemEdit : function(data) {
            _initPageItemEdit(data);
        },
        addPageItemEdit : function(data) {
            _addPageItemEdit(data);
        },
        formatPageItemEdit : function(data) {
            var PageItem = {
                PageItemId : data.PageItemId,
                PageItemName : data.PageItemName,
                PageItemCycle : 0
            };
            return PageItem;
        },
        delPageItemEdit : function(data) {
            $('#page-item-chooser-edit td[page-item-id=' + data.PageItemId + ']')
                    .parent().remove();
            _disappearPageItemEdit();
        },
        getPageItemEdit : function() {
            return _getPageItemEdit();
        }
    };
});