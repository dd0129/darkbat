var userVerify = new Global.UserVerify();

var createBreadcrumb = function() {
	new FC.Breadcrumb($('#bread-crumb'), {
		siteStruction: {
			secondLevel: {
				title: '调度系统',
				subSites: [{
						title: '调度监控',
						url: 'task-monitor.html'
					},
					{
						title: '查看任务',
						url: 'task-view.html'
					},
					{
						title: '新增任务',
						url: 'task-add.html'
					},
					{
						title: 'AutoETL',
						url: 'autoetl-query.html'
					},
					{
						title: 'Hive建表工具',
						url: 'hive-tool.html'
					},{
                        title:'SLA实时监控',
                        url: 'slaJob-status.html'
                    }
				]
			}
		},
		subSiteIndex:1
	});
};

$(function() {
	userVerify.verifyLogin(function() {
		createBreadcrumb();
		comm.addOnDutyInfo();
		firstSearch();
	});
	
	for (var i = 0; i < Global.AllTeam.length; ++i) {
        $('#selector-owner').append($('<option>').attr('value', Global.AllTeam[i][0]).text(Global.AllTeam[i][1]));
    }
	
	$('#search-btn').click(function(event) {
		$('#result-info').empty();
		selectorFormSearch();
		event.preventDefault();
	});
});

var firstSearch = function() {
    var taskId = FC.getQueryStringByName('taskId');
	var pinyinName = userVerify.getPinyinName();
	var ownerVal = '';
	$.each($('#selector-owner').children(), function() {
		if (this.value == pinyinName) {
			ownerVal = this.value;
			$('#selector-owner').val(ownerVal);
			return false;
		}
	});
	var valid = '1';
	$('#selector-if-val').val(valid);
	searchTask({
		taskId: taskId ? taskId : '',
		taskGroupId: '',
		cycle: '',
		owner: taskId ? '' : ownerVal,
		databaseSrc: '',
		ifVal: valid,
		taskName: '',
		pageSize: 20,
		pageNo: 1,
		pageSort: '',
		isWin: 0
	});
};
