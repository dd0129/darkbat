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
					}
				]
			}
		},
		subSiteIndex:4
	});
};

var htmlEscape = function(str) {
	return String(str)
				.replace(/&/g, '&amp;')
				.replace(/"/g, '&quot;')
				.replace(/'/g, '&#39;')
				.replace(/</g, '&lt;')
				.replace(/>/g, '&gt;');
};

var getACLGroup = function() {
	$.ajax({
		url: '/darkbat/json/getACLGroupAction',
		async: false,
		type: 'post',
		dataType: 'json',
		data: {
		}
	}).done(function(data) {
				var online = data.online;
				var offline = data.offline;
				$('#online_group').append(new Option());
				$(online).each(function(i,e) {
					$('#online_group').append(new Option(e,e));
				});

				$('#offline_group').append(new Option());
				$(offline).each(function(i,e) {
					$('#offline_group').append(new Option(e,e));
				});
				console.log(data.online);
				console.log(data.offline);
				console.log(data.code);

	}).fail(function(data) {

	});
};

$(function() {
	userVerify.verifyLogin(function() {
		createBreadcrumb();
		comm.addOnDutyInfo();
		
		if (!userVerify.isAllTeam()) {
			$('#submit-btn').attr('disabled', 'disabled');
		}
		var pinyinName = userVerify.getPinyinName();
		var ownerVal = '';
		$.each($('#owner').children(), function() {
			if (this.value == pinyinName) {
				ownerVal = this.value;
				$('#owner').val(ownerVal);
				return false;
			}
		});
		getACLGroup();
	});

	for (var i = 0; i < Global.AllTeam.length; ++i) {
	    $('#owner').append($('<option>').attr('value', Global.AllTeam[i][0]).text(Global.AllTeam[i][1]));
	}


	$('#search-btn').click(function(event) {
		$('#result-info').empty();
		var databaseName = $('#selector-database-name').val();
		var tableName = $('#selector-table-name').val();
		if (databaseName == '' || tableName == '') {
			new FC.MessageLoader($('#result-info'), {
				msgClassName: 'alert-error',
				msg: '请输入查询条件',
				keepOnly: true,
				closeTime: 5000
			});
		} else {
			$.ajax({
				url: '/darkbat/json/generateTabInfoAction',
				async: false,
				type: 'post',
				dataType: 'json',
				data: {
					databaseName: databaseName,
					tableName: tableName
				}
			}).done(function(data) {
				if (data.code != 200) {
					new FC.MessageLoader($('#result-info'), {
						msgClassName: 'alert-error',
						msg: data.msg,
						keepOnly: true
					});
					window.location.href = window.location.href.replace(/#.*/gi, '') + '#top';
					return;
				}
				var entity = data.msg;
				var columnEntity = entity.columnEntity;
				var partitionKey = entity.partitionKey;
				var tableInfo = entity.tableInfo;
				$('#database').val(databaseName);
				$('#table-comment').val(tableInfo.tableComment);
				$('#table-type').val(tableInfo.tableType);
				$('#serde-library').val(tableInfo.tableSerde);
				$('#input-format').val(tableInfo.tableInputformat);
				$('#output-format').val(tableInfo.tableOutputformat);
				$('#num-buckets').val(tableInfo.tableBucketsNum);
				$('#bucket-columns').val(tableInfo.tableBucketscol);
				$('#sort-columns').val(tableInfo.tableSortcol);
				$('#collection-delim').val(tableInfo.tableColelctiondelim);
				$('#field-delim').val(tableInfo.tableFielddlim);
				$('#line-delim').val(tableInfo.tableLinedelim);
				$('#mapkey-delim').val(tableInfo.mapKeydelim);
				$('#serialization-format').val(tableInfo.serializationFormat);
				$('#column-table-body').empty();

				if (tableInfo.tableType.toUpperCase() != "MANAGED_TABLE") {
					$('#location').attr("readonly",false).attr("placeholder","hdfs://10.1.77.86/user/hive/...").val(tableInfo.tableLocation);
				} else{
					$('#location').val(tableInfo.tableLocation).attr("readonly",true);
				}

				$.each(columnEntity, function() {
					$('#column-table-body')
				    	.append($('<tr>')
				    		.append($('<td>').attr('style', 'width:10%;text-align:center;').append(htmlEscape(this.columnKey)))
				    		.append($('<td>').attr('style', 'width:30%;text-align:center;').append(htmlEscape(this.columnName)))
				    		.append($('<td>').attr('style', 'width:15%;text-align:center;').append(htmlEscape(this.columnType)))
				    		.append($('<td>').attr('style', 'width:45%;text-align:center;').append(
				    			$('<input>').attr('type', 'text').attr('class', 'input-xlarge').attr('name', 'column-comment').attr('value', this.columnComment)		
				    		))
				    );
				});
				idx = 1;
				$('#partition-column-table-body').empty();
				$.each(partitionKey, function() {
					$('#partition-column-table-body')
				    	.append($('<tr>')
				    		.append($('<td>').attr('style', 'width:10%;text-align:center;').append(htmlEscape(this.columnKey)))
				    		.append($('<td>').attr('style', 'width:30%;text-align:center;').append(htmlEscape(this.columnName)))
				    		.append($('<td>').attr('style', 'width:15%;text-align:center;').append(htmlEscape(this.columnType)))
				    		.append($('<td>').attr('style', 'width:45%;text-align:center;').append(
				    			$('<input>').attr('type', 'text').attr('class', 'input-xlarge').attr('name', 'partition-column-comment').attr('value', this.columnComment)		
				    		))
				    );
				});
			}).fail(function(jqXHR, textStatus) {
				new FC.MessageLoader($('#result-info'), {
					msgClassName: 'alert-error',
					msg: '系统出错，' + jqXHR.responseText,
					keepOnly: true
				});
				window.location.href = window.location.href.replace(/#.*/gi, '') + '#top';
			});
		}
		event.preventDefault();
	});
	
	$('#submit-btn').click(function(event) {
		var databaseName = $('#selector-database-name').val();
		var tableName = $('#selector-table-name').val();
		var owner = $('#owner').val();
		var storageCycle = $('#storage-cycle').val();
		var tableComment = $('#table-comment').val();
		var location = $('#location').val();
		var columnComment = '';
		var columnSize = 0;
		$.each($('[name="column-comment"]'), function() {
			columnComment += this.value + '(~.~)';
			++columnSize;
		});
		var partitionColumnComment = '';
		var partitionColumnSize = 0;
		$.each($('[name="partition-column-comment"]'), function() {
			partitionColumnComment += this.value + '(~.~)';
			++partitionColumnSize;
		});
		$.ajax({
			url: '/darkbat/json/generateAutoBuildTabDDLAction',
			async: false,
			type: 'post',
			dataType: 'json',
			data: {
				databaseName: databaseName,
				tableName: tableName,
				owner: owner,
				storageCycle: storageCycle,
				tableComment: tableComment,
				columnComment: columnComment,
				columnSize: columnSize,
				partitionColumnComment: partitionColumnComment,
				partitionColumnSize: partitionColumnSize,
				location: location
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
			$('#sql-modal').modal({
				keyboard: false,
                backdrop: 'static'
			}).css({
				width: '1000px',
				'margin-left': function () {
					return -($(this).width() / 2);
				},
				'max-height': '90%',
				'margin-top': function() {
					if ($.browser.msie) {
						return '-23%';
					} else if ($.browser.mozilla) {
						return '-23%';
					}
					return '-23%';
				}
			}).show();
			var sqls = data.msg == '' ? '预发布环境和线上环境中的表结构相同！' : data.msg;
			$('#sql-preview').html(htmlEscape(sqls));
			if (data.msg == '') {
				$('#confirm-btn').unbind('click').click(function() {
					$('#sql-modal').modal('hide');
				});
			} else {
				$('#confirm-btn').unbind('click').click(function() {
					new FC.MessageLoader($('#result-info'), {
						msgClassName: 'alert-info',
						msg: '建表需要一定时间，请耐心等待！',
						keepOnly: true
					});
					window.location.href = window.location.href.replace(/#.*/gi, '') + '#top';
					$('#submit-btn').attr('disabled', 'disabled');
					$('#sql-modal').modal('hide');
					var databaseName = $('#selector-database-name').val();
					var tableName = $('#selector-table-name').val();
					var owner = $('#owner').val();
					var storageCycle = $('#storage-cycle').val();
					var tableComment = $('#table-comment').val();
					var auth_online = $('#online_group').val();
					var auth_offline = $('#offline_group').val();
					var location = $('#location').val();
					var columnComment = '';
					var columnSize = 0;
					$.each($('[name="column-comment"]'), function() {
						columnComment += this.value + '(~.~)';
						++columnSize;
					});
					var partitionColumnComment = '';
					var partitionColumnSize = 0;
					$.each($('[name="partition-column-comment"]'), function() {
						partitionColumnComment += this.value + '(~.~)';
						++partitionColumnSize;
					});
					$.ajax({
						url: '/darkbat/json/createTableAutoBuildTabAction',
						type: 'post',
						dataType: 'json',
						data: {
							databaseName: databaseName,
							tableName: tableName,
							owner: owner,
							mail:userVerify.getUserEmail(),
							storageCycle: storageCycle,
							tableComment: tableComment,
							columnComment: columnComment,
							columnSize: columnSize,
							partitionColumnComment: partitionColumnComment,
							partitionColumnSize: partitionColumnSize,
							auth_online:auth_online,
							auth_offline:auth_offline,
							location: location
						}
					}).done(function(data) {
						$('#submit-btn').removeAttr('disabled');
						if (data.code != 200) {
							new FC.MessageLoader($('#result-info'), {
								msgClassName: 'alert-error',
								msg: '系统出错，' + data.msg,
								keepOnly: true
							});
							window.location.href = window.location.href.replace(/#.*/gi, '') + '#top';
							return;
						}
						new FC.MessageLoader($('#result-info'), {
							msgClassName: 'alert-info',
							msg: '建表成功\n 表授权请求已经提交处理，10分钟内生效',
							keepOnly: true
						});
						window.location.href = window.location.href.replace(/#.*/gi, '') + '#top';
					}).fail(function(jqXHR, textStatus) {
						$('#submit-btn').removeAttr('disabled');
						new FC.MessageLoader($('#result-info'), {
							msgClassName: 'alert-error',
							msg: '系统出错，' + jqXHR.responseText,
							keepOnly: true
						});
						window.location.href = window.location.href.replace(/#.*/gi, '') + '#top';
					});
				});
			}
		}).fail(function(jqXHR, textStatus) {
			new FC.MessageLoader($('#result-info'), {
				msgClassName: 'alert-error',
				msg: '系统出错，' + jqXHR.responseText,
				keepOnly: true
			});
			window.location.href = window.location.href.replace(/#.*/gi, '') + '#top';
		});
		event.preventDefault();
	});
});