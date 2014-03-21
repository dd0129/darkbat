define(function(require) {
    require('jquery');
    
    var 
        modalUtil   = require('localJs/common/util/modalWithId'),
        FC          = FC||require('halleyCommon')
    ;

    var _controllerOptions = {
        id: 'unknown',
        name: 'unknown',
        placeholder: 'unknown',
        btnName: 'unknown',
        dataKey: 'unknown',
        $container: null,
        inputClass: 'input-big',
        initController: function($controller) {
            return;
        }
    };

    var _modalOptions = {
        modalId: 'unknown',
        message: 'unknown',
        innerHTML: 'unknown',
        initModal: null,
        onSubmit: null,
        cssOptions: null,
        isRefresh: false
    };

    function _buildControl(controllerOptions, modalOptions) {
        var html = '' +
            '<div class="control-group" id="' + controllerOptions.id + '">' +
                '<label class="control-label">' + controllerOptions.name + '</label>' +
                '<div class="controls">' +
                    '<a class="btn btn-primary">' + controllerOptions.btnName + '</a>' +
                '</div>' +
            '</div>';

        var $div = $(html);
        $div.find('a.btn').click({
            modalOptions: modalOptions
        }, _showModal);
        
        $('#mail-confirm').addClass('disabled');
        $div.find('a.btn').addClass('disabled');
        _buildModal(modalOptions);
        var $oldModal = $('#' + modalOptions.modalId);
        $oldModal.modal('hide');
        
        controllerOptions.initController($div);
        controllerOptions.$container.append($div);
    }

    function _showModal(e) {
        var modalOptions = e.data.modalOptions;
        
        if(modalOptions.isRefresh) {            
            _buildModal(modalOptions);
        } else {
            var $oldModal = $('#' + modalOptions.modalId);
            if($oldModal.length) {
                modalUtil.showMessage({
                    isRefresh: false,
                    modalId: modalOptions.modalId
                });
            } else {
                _buildModal(modalOptions);
            }
        }
    }
    function _buildModal(modalOptions) {

        var $pDiv = modalUtil.showMessage({
            message: modalOptions.message,
            innerHTML: modalOptions.innerHTML,
            hasSubmit: true,
            onSubmit: function() {
                var close = false;
                if(modalOptions.onSubmit) {
                    close = modalOptions.onSubmit($pDiv);
                }

                if(close) {
                    $pDiv.modal('hide');
                }
            },
            isRefresh: modalOptions.isRefresh,
            modalId: modalOptions.modalId
        });

        modalUtil.fixCss($pDiv, modalOptions.cssOptions);

        if(modalOptions.initModal) {
            modalOptions.initModal($pDiv);
        }
    }

    return {
        /**
         * 初始化选择控件
         * @param  {[type]} controllerOptions [description]
         * @param  {[type]} modalOptions      [description]
         * @return {[type]}                   [description]
         */
        initControl: function(controllerOptions, modalOptions) {
//            controllerOptions = FC.setOptions(_controllerOptions, controllerOptions);
//            modalOptions = FC.setOptions(_modalOptions, modalOptions);
            //TODO 优化参数传递
            _buildControl(controllerOptions, modalOptions);
        }
    };
});