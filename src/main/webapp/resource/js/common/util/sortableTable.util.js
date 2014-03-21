define(function(require) {
    
    function _initSortableTable (tableId, hasDelete, deleteCallBackFunc, hasSort, hasHide, hides) {

        _clear(tableId);
        
        $(tableId).find('thead').find('tr').append('<th class="operate-th">操作</th>');

        var $tbody=$(tableId).find('tbody'),
            $trs = $tbody.find('tr');
        
        var deleteHtml = hasDelete ? '&nbsp;&nbsp;' + '<a class="btn CLICKABLE btn-delete btn-info">删除</a>' : '';
        var sortHtml = hasSort == false ? '' : '' +
                    '<a class="btn CLICKABLE btn-sort btn-up btn-info">▲</a>' +
                    '&nbsp;&nbsp;'+
                    '<a class="btn CLICKABLE btn-sort btn-down btn-info">▼</a>';
        var hideHtml = hasHide ? '&nbsp;&nbsp;' + '<a class="btn CLICKABLE btn-hide btn-info">隐藏</a>' : '';
        
        var operateHtml = '<td class="operate-td">' + sortHtml + deleteHtml + hideHtml + '</td>';
        
        $trs.each(function (i, e) {
            $(e).append(operateHtml);
        });

        _resetDisabled(tableId);
        _resetHide(tableId);

        //调整参数的顺序
        $tbody.find('.btn-sort').click({length: $trs.length}, function(e) {
            var $me = $(this);
            if ($me.hasClass('disabled')) {
                return;
            }
            
            var $tr = $me.parents('tr:first');
            var index = $tr.parents('tbody:first').find('tr').index($tr) + 1;
            
            if ($me.hasClass('btn-up')) {
                //按上升按钮
                var $prevTr = $tr.prev().attr('data-display-index', index);
                $tr.insertBefore($prevTr);
                $tr.attr('data-display-index', index - 1);
            } else {
                //按下降按钮
                var $nextTr = $tr.next().attr('data-display-index', index);
                $tr.insertAfter($nextTr);
                $tr.attr('data-display-index', index + 1);
            }
            _resetDisabled(tableId);
            _resetHide(tableId);
        });
        
        //删除按钮click事件
        $tbody.find('.btn-delete').click({
            deleteCallBackFunc: typeof deleteCallBackFunc != 'undefined' ? deleteCallBackFunc : null
        }, function(event) {
             var $btnDelete = $(this);
             var $tr = $btnDelete.parents('tr:first');
             $tr.remove();
             if (event.data.deleteCallBackFunc) {
                 event.data.deleteCallBackFunc();
             }
             _resetDisabled(tableId);
             _resetHide(tableId);
        });
        
        //隐藏按钮click事件
        $tbody.find('.btn-hide').click({
            hideCallBackFunc: typeof hideCallBackFunc != 'undefined' ? hideCallBackFunc : null
        }, function(event) {
             var $btnHide = $(this);
             var $tr = $btnHide.parents('tr:first');
             if ($btnHide.text() == '隐藏') {
                 $btnHide.text('显示');
                 $tr.attr('isHide', true);
             } else {
                 $btnHide.text('隐藏');
                 $tr.attr('isHide', false);
             }
             if (event.data.hideCallBackFunc) {
                 event.data.hideCallBackFunc();
             }
             _resetDisabled(tableId);
             _resetHide(tableId);
        });

        if (hasHide) {
            var $btns = $tbody.find('.btn-hide');

            //执行默认隐藏
            $(hides).each(function(i, e) {
                var $btn = $($btns[e]).click();
            });
            
        }
    }
    
    function _resetDisabled(tableId) {
        var $tbody = $(tableId).find('tbody');
        $tbody.find('.btn-sort').removeClass('disabled');
        $tbody.find('tr:first').find('.btn-up').addClass('disabled');
        $tbody.find('tr:last').find('.btn-down').addClass('disabled');
    }
    function _resetHide(tableId) {
        var $tbody = $(tableId).find('tbody');
        $tbody.find('.btn-hide').each(function() {
            var $btnHide = $(this);
            var $tr = $btnHide.parents('tr:first');
            if ($tr.attr('isHide') == 'true') {
                $btnHide.text('显示');
            }
        });
    }
    
    function _clear(tableId) {
        $(tableId).find('.operate-th').remove();
        $(tableId).find('.operate-td').remove();
    }
    
    return {
        /**
         * options:    {
         *             tableId:
         *             hasDelete:
         *             deleteCallBackFunc:
         *             hides:
         *          }
         */
        init: function(options) {
            options.hides = options.hides || [];
            _initSortableTable(options.tableId, options.hasDelete, options.deleteCallBackFunc, options.hasSort, options.hasHide, options.hides);
        }
    };
});
