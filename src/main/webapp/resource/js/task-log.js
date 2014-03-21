var userVerify = new Global.UserVerify();

var showLog = function() {
	var logPath = FC.getQueryStringByName('logPath');

	$.ajax({
		url : '/darkbat/json/getTaskLogContentAction',
		data : {
			logPath : logPath
		},
		type : 'post',
		dataType : 'json',
		success : function(cb) {
			if (cb.code === 200) {
				$('#logContent').html(cb.msg);
			} else {
				$('#logContent').text("读取日志文件失败!");
			}
		},
		error: function() {
			$('#logContent').text("读取日志文件失败!");
		}
	});
};

$(function() {
	showLog();
});