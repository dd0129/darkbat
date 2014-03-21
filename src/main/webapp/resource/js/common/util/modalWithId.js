define(function(require) {
    require('bootstrap');

    var msgOptions = {
        message: 'unknow',
        innerHTML: '',
        hasClose:true,
        hasSubmit: false,
        submitText:'确定',
        onSubmit: null,
        modalId: 'unknown',
        isRefresh: true
    };
    var cssOptions = {
        'margin':     0,
        'top':        '100px',
        'position':   'absolute',
        'width':      '560px',
        'left':       '50%',
        'margin-left':'-280px'
    };
    
    function modalDiv($div) {
        $div.modal({
            backdrop: 'static'
        }).show();
        
        return $div;
    }

    function initModalDiv(options) {
        var $div = $(FC.getHtmlTemplate('resource/html/util/message.html', options));
        if(!options.hasClose){
            $div.find('.modal-footer .btn').remove();
        }
        if(options.hasSubmit) {
            var $btn = $div.find('.modal-footer').append('<a href="#" class="btn btn-primary" submit-btn>' + options.submitText + '</a>');
            
            if(options.onSubmit) {
                $btn.click({
                    $modal: $div
                }, function(e) {
                    options.onSubmit(e.data.$modal);
                });
            }
        }
        return modalDiv($div);
    }

    function removeModal($modal) {
        $modal.modal('hide');
        $modal.remove();
    }
    
    return {
        showMessage: function(options) {
            options = FC.setOptions(msgOptions, options);
            var $oldModal = $('#' + options.modalId),
                hasOldModal = ($oldModal.length > 0);

            if(options.isRefresh) {
                //刷新modal弹框
                if(hasOldModal) {
                    removeModal($oldModal);
                }
                return initModalDiv(options);
                
            } else {
                if(hasOldModal) {
                    modalDiv($oldModal);
                } else {
                    return initModalDiv(options);
                }
            }
        },

        removeModal: removeModal,

        fixCss: function($div, options) {
            options = FC.setOptions(cssOptions, options);

            var reg = /\d+/;
            var width = parseInt(reg.exec(options['width'])[0]);
            options['margin-left'] = '-' + (width/2) + 'px';

            $div.css(options);
        }
    };
});