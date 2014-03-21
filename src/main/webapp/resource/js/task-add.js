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
		subSiteIndex:2
	});
};

$(function() {
	userVerify.verifyLogin(function() {
		if (!userVerify.isAllTeam()) {
			$('#submit-btn').attr('disabled', 'disabled');
		}
		createBreadcrumb();
		comm.addOnDutyInfo();
		var basic = $('#basic');
		for (var i = 0; i < Global.AllTeam.length; ++i) {
	        basic.find('[name=owner]').append($('<option>').attr('value', Global.AllTeam[i][0]).text(Global.AllTeam[i][1]));
	    }
		
		var pinyinName = userVerify.getPinyinName();
        var ownerVal = '';
        $.each(basic.find('[name=owner]').children(), function() {
            if (this.value == pinyinName) {
                ownerVal = this.value;
                basic.find('[name=owner]').val(ownerVal);
                return false;
            }
        });
	});
	
	$('#submit-btn').click(addOrModifyTask);

	$('#add-row-btn').click(openWin);

	var basic = $('#basic');
	basic.find('[name=database-src]').change(checkTaskName);
	basic.find('[name=table-name]').blur(checkTaskName);
	basic.find('[name=freq]').blur(checkFreq);
	basic.find('[name=recall-code]').blur(checkRecallCode);
	basic.find('[name=wait-code]').blur(checkWaitCode);
	basic.find('[name=success-code]').blur(checkSuccessCode);
	basic.find('[name=offset-type]').change(changeOffsetType);
	basic.find('[name=if-recall]').change(changeIfRecall);
	basic.find('[name=if-wait]').change(changeIfWait);
	
	var tableName = basic.find('[name=table-name]');
	var tableNameControlGroup = tableName.parents('.control-group');
	tableNameControlGroup.addClass('error');
	tableName.parent().append($('<span>').attr('class', 'help-inline error').text('请填写结果表名'));
	
	$('#dol-btn').click(getDolInfo);
});