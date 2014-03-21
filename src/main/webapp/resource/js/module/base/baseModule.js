define(function (require) {
    var comm = require('localJs/common/common');

    var 
        _isOnline     = comm.isOnline,
        _token        = '',
        _tokenPattern = ''
    ;
        
    var _options = {
        basePath: '',
        isRestful: true
    };

    /**
     * [_baseDoOne description]
     * @param  {[type]} json    [description]
     * @param  {[type]} options {
     *                              url: 
     *                              onSuccess:
     *                              onError:
     *                          }
     * @return {[type]}         [description]
     */
    function _baseDoQuery(json, options) {

        $.ajax({
            url: options.url + _tokenPattern,
            data: json,
            type: options.type || 'POST',
            dataType: options.dataType || 'json',
            success: options.onSuccess,
            error: function(cb) {
                if(options.onError) {
                    options.onError(cb);
                }
            }
        });
    }

    return {
        /**
         * 对象初始化
         * @param  {[type]} token   [description]
         * @param  {[type]} options {
         *                              basePath: 
         *                              isRestful: true
         *                          }
         * @return {[type]}         [description]
         */
        init: function(token, options) {
            _token = token;
            _tokenPattern = '?token=' + token;
        },

        /**
         * 增、删、改、查一个元素
         * @type {[type]}
         */
        addOne: _baseDoQuery,
        deleteOne: _baseDoQuery,
        getOne: _baseDoQuery,
        updateOne: _baseDoQuery,

        getList: _baseDoQuery,

        doQuery: _baseDoQuery
    };

});