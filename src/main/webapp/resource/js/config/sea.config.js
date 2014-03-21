var userVerify;
var localUrl = 'http://' + window.location.host;
var isOnline = localUrl === 'http://data.dp';

seajs.config({
    // plugins: isOnline ? ['text', 'shim'] : ['text', 'shim', 'nocache'],

    plugins: ['text', 'shim'],
    alias: {
        'jquery': {
            src: 'comResrcJs/jquery-1.7.2.min.js',
            exports: 'jQuery'
        },

        '$EasyUi': {
            src: 'comResrcJs/jquery-easyui/jquery.easyui.min.js',
            deps: ['jquery']
        },
        '$smartWizard': {
            src: 'comResrcJs/jquery.smartWizard-2.0.js',
            deps: ['jquery']
        },

        'bootstrap': {
            src: 'comResrcJs/bootstrap.js',
            deps: ['jquery']
        },

        'datepicker': {
            src: 'comResrcJs/bootstrap-datepicker.js',
            deps: ['jquery']
        },

        'halleyCommon': {
            src: 'comResrcJs/halley-common.js',
            deps: ['jquery'],
            exports: 'FC'
        },
        'halleyGlobal': {
            src: 'comResrcJs//halley-global.js',
            deps: ['jquery'],
            exports: 'Global'
        },
        'halleyPrototype': {
            src: 'comResrcJs/halley-prototype.js',
            deps: ['jquery']
        },
        'zTree' : {
            src: 'comResrcJs/jquery.ztree.all-3.5.min.js',
            deps: ['jquery']
        },
        
        'common' : 'localJs/common/common.js'
    },
    
    paths: {
        // 'comResrcJs': isOnline ? 'http://data.dp/pluto/resource/js' : localUrl + '/pluto/resource/js',
        'comResrcJs': 'http://data.dp/pluto/resource/js',
        'localJs': isOnline ? localUrl + '/halley/resource/js' : localUrl + '/darkbat/resource/js',
        'localSrc': isOnline ? localUrl + '/halley/resource' : localUrl + './darkbat/resource'
    },
    

    // 调试模式
    debug: isOnline ? false : true,

    // 文件编码
    charset: 'utf-8'
});