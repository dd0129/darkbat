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
		subSiteIndex:3
	});
};

$(function() {
	userVerify.verifyLogin(function() {
		createBreadcrumb();
		comm.addOnDutyInfo();
	});
	$('#selector-datasource-type').val('');
	$('#selector-database-name').val('');

	$('#search-btn').click(function(event) {
		$('#result-info').empty();
		var dsSelector = $('#selector-datasource-type');
		dsSelector.next().remove();
		if (dsSelector.val() == '') {
			new FC.MessageLoader($('#result-info'), {
				msgClassName: 'alert-error',
				msg: '请选择数据源',
				keepOnly: true,
				closeTime: 5000
			});
		} else {
			selectorFormSearch();
		}
		event.preventDefault();
	});
	
	$('#selector-datasource-type').change(function() {
		var datasourceType = $('#selector-datasource-type').val();
		if (datasourceType == 'hive' || datasourceType == 'mysql') {
			$.ajax({
				url: '/darkbat/json/getAllDatabaseAction',
				async: false,
				type: 'post',
				dataType: 'json',
				data: {
					datasourceType: datasourceType
				}
			}).done(function(data) {
				if (data.code != 200) {
					new FC.MessageLoader($('#result-info'), {
						msgClassName: 'alert-error',
						msg: '系统出错，' + data.msg,
						keepOnly: true
					});
					window.location.href = window.location.href.replace(/#.*/gi, '') + '#top';
					return;
				}
				$('#selector-database-name').empty();
				$('#selector-database-name').append($('<option>').attr('value', '').text('--选择全部--'));
				$.each(data.msg, function() {
					$('#selector-database-name').append($('<option>').attr('value', this).text(this));
				});
			}).fail(function(jqXHR, textStatus) {
				new FC.MessageLoader($('#result-info'), {
					msgClassName: 'alert-error',
					msg: '系统出错，' + jqXHR.responseText,
					keepOnly: true
				});
				window.location.href = window.location.href.replace(/#.*/gi, '') + '#top';
			});
		} else if (datasourceType == 'sqlserver') {
			$('#selector-database-name').empty();
			$('#selector-database-name')
				.append($('<option>').attr('value', 'zSurvey_NET').text('zSurvey_NET'))
				.append($('<option>').attr('value', 'DianPingFinance').text('DianPingFinance'))
				.append($('<option>').attr('value', 'DianPingCommon').text('DianPingCommon'))
				.append($('<option>').attr('value', 'DianPingPM').text('DianPingPM'))
                .append($('<option>').attr('value', 'DPFinance').text('DPFinance'))
                .append($('<option>').attr('value', 'DP_BPM_K2Sln').text('DP_BPM_K2Sln'))
				.append($('<option>').attr('value', 'DianPingCRM').text('DianPingCRM'))
				.append($('<option>').attr('value', 'DianPingKM').text('DianPingKM'));
		} else if (datasourceType == 'greenplum') {
			$('#selector-database-name').empty();
			$('#selector-database-name').append($('<option>').attr('value', '').text('dianpingdw57'));
		} else {
			$('#selector-database-name').empty();
			$('#selector-database-name').append($('<option>').attr('value', '').text('--选择全部--'));
		}
	});
});

var refreshTable = function(data) {
	if (data.msg.count == 0) {
		new FC.MessageLoader($('#result-info'), {
			msgClassName: 'alert-error',
			msg: '没有符合条件的结果',
			keepOnly: true,
			closeTime: 2500
		});
	}
	var tables = data.msg.tables;
	$('#result-body').empty();
	$.each(tables, function() {
		$('#result-body')
	    	.append($('<tr>')
    			.append($('<td>')
					.attr('style', 'width:15%;')
					.append(this.datasourceType))
	    		.append($('<td>')
    				.attr('style', 'width:20%;')
    				.append($('<div>')
						.attr('style', 'text-align:left;')
						.append(this.databaseName)))
	    		.append($('<td>')
    				.attr('style', 'width:30%;')
    				.append($('<div>')
						.attr('style', 'text-align:left;')
						.append(this.schemaName == '' ? this.tableName : this.schemaName + '.' + this.tableName)))
	    		.append($('<td>')
    				.attr('style', 'width:15%;')
    				.append(this.tableRows))
    				.append($('<td>')
    						.attr('style', 'width:20%;')
    						.append( 
    								(this.onSchedule.indexOf('hive') == -1 ? '-' : 'hive') + 
									'/' + 
									(this.onSchedule.indexOf('gp') == -1 ? '-' : 'gp') +
                                    '/' +
									(this.onSchedule.indexOf('gp_analysis') == -1 ? '-' : 'analysis') +
									'/' +
									(this.onSchedule.indexOf('gp_report') == -1 ? '-' : 'report')
							)
					)
	    		.append($('<td>')
    				.attr('style', 'width:10%;')
	    			.append($('<button>')
	    				.attr('class', 'btn btn-mini')
	    				.click({datasourceType: this.datasourceType, databaseName: this.databaseName, schemaName: this.schemaName, tableName: this.tableName}, redirectAdd)
	    				.append($('<i>').attr('class', 'icon-ok'))))
	    );
	});
	$('#result-body').hide().fadeIn();
};

var redirectAdd = function(args) {
	$(window).attr('location', 
			'autoetl-add.html?datasourceType=' + args.data.datasourceType + 
			'&databaseName=' + args.data.databaseName + 
			'&schemaName=' + args.data.schemaName +
			'&tableName=' + args.data.tableName);
};

var selectorFormSearch = function() {
	searchTable({
		datasourceType: $('#selector-datasource-type').val(),
		databaseName: $('#selector-database-name').val(),
		tableName: $('#selector-table-name').val(),

		pageSize: 20,
		pageNo: 1
	});
};

var searchTable = function(args) {
	$.ajax({
		url: '/darkbat/json/searchTableAction',
		async: false,
		type: 'post',
		dataType: 'json',
		data: {
			datasourceType: args.datasourceType,
			databaseName: args.databaseName,
			tableName: args.tableName,

			pageSize: args.pageSize,
			pageNo: args.pageNo
		}
	}).done(function(data) {
		if (data.code != 200) {
			new FC.MessageLoader($('#result-info'), {
				msgClassName: 'alert-error',
				msg: '系统出错，' + data.msg,
				keepOnly: true
			});
			window.location.href = window.location.href.replace(/#.*/gi, '') + '#top';
			return;
		}

		refreshTable(data);

		new FC.Pager($('.pager-ele'), {
			currentPageNo: args.pageNo,
			lastPageNo: Math.ceil(data.msg.count/args.pageSize),
			onClick: function(pageNo) {
				searchTable({
					datasourceType: args.datasourceType,
					databaseName: args.databaseName,
					tableName: args.tableName,

					pageSize: args.pageSize,
					pageNo: pageNo
				});
			}
		});
	}).fail(function(jqXHR, textStatus) {
		new FC.MessageLoader($('#result-info'), {
			msgClassName: 'alert-error',
			msg: '系统出错，' + jqXHR.responseText,
			keepOnly: true
		});
		window.location.href = window.location.href.replace(/#.*/gi, '') + '#top';
	});
};