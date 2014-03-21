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


var datasourceType = $.url().param('datasourceType');
var databaseName = $.url().param('databaseName');
var schemaName = $.url().param('schemaName');
var tableName = $.url().param('tableName');
var tableNameSub = tableName.substring(tableName.indexOf('.') + 1).toLowerCase();
var tableNameLow = tableName.toLowerCase();
var columns;


$(function() {
    
	userVerify.verifyLogin(function() {
		if (!userVerify.isDWTeam()) {
			$('#submit-btn').attr('disabled', 'disabled');
		}
		createBreadcrumb();
		comm.addOnDutyInfo();
	});
	
	$('#target-datasource-type').val('');
	
	$.ajax({
		url: '/darkbat/json/getAllColumnAction',
		type: 'post',
		dataType: 'json',
		data: {
			datasourceType: datasourceType,
			databaseName: databaseName,
			schemaName: schemaName,
			tableName: tableName
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
		columns = data.msg.columns;
		$('#column-body').empty();
		if (columns == '') {
			new FC.MessageLoader($('#result-info'), {
				msgClassName: 'alert-info',
				msg: '该表无字段',
				keepOnly: true
			});
			window.location.href = window.location.href.replace(/#.*/gi, '') + '#top';
		} else {
			$.each(columns, function() {
				$('#column-body')
			    	.append($('<tr>')
		    			.attr('class', 'bg-white')
			    		.append($('<td>').attr('style', 'width:25%;line-height:15px;text-align:left;').append(this.columnName))
			    		.append($('<td>').attr('style', 'width:20%;line-height:15px;').append(this.columnType))
			    		.append($('<td>').attr('style', 'width:10%;line-height:15px;').append(this.columnKey))
			    		.append($('<td>').attr('style', 'width:45%;line-height:15px;text-align:left;').append(this.columnComment))
			    );
			});
		}
		$('#column-body').hide().fadeIn();
	}).fail(function(jqXHR, textStatus) {
		new FC.MessageLoader($('#result-info'), {
			msgClassName: 'alert-error',
			msg: '系统出错，' + jqXHR.responseText,
			keepOnly: true
		});
		window.location.href = window.location.href.replace(/#.*/gi, '') + '#top';
	});

	$('#target-is-active-schedule').change(targetIsActiveScheduleChange);
	$('#target-datasource-type').change(targetDatasourceTypeChange);
	$('#target-table-type').change(targetTableTypeChange);
    //$('#target-write-type').change(targetWriteTypeChange);


	$('#cancel-btn').click(function(event) {
		window.location.href = "autoetl-query.html";
	});
	
	$('#submit-btn').click(function(event) {
		$('#sql-preview').html('');
		var targetIsActiveScheduleVal = $('#target-is-active-schedule').val();
		var targetDatasourceTypeVal = $('#target-datasource-type').val();
		var targetSchemaTableVal = $('#target-schema-table').val();
		var targetTableTypeVal = $('#target-table-type').val();
		var targetSegmentColumnVal = $('#target-segment-column').val();
        var targetWriteType = $('#target-write-type').val();

		if (targetIsActiveScheduleVal == '' || targetDatasourceTypeVal == '' || targetSchemaTableVal == '' || targetTableTypeVal == '') {
			new FC.MessageLoader($('#result-info'), {
				msgClassName: 'alert-error',
				msg: '请将目标表配置信息填写完整',
				keepOnly: true
			});
		} else {
			$.ajax({
				url: '/darkbat/json/generateDDLAction',
				type: 'post',
				dataType: 'json',
				data: {
					datasourceType: datasourceType,
					databaseName: databaseName,
					schemaName: schemaName,
					tableName: tableName,
					targetIsActiveSchedule: targetIsActiveScheduleVal,
					targetDatasourceType: targetDatasourceTypeVal,
					targetSchemaTable: targetSchemaTableVal,
					targetTableType: targetTableTypeVal,
					targetSegmentColumn: targetSegmentColumnVal
                    //targetWriteType : targetWriteType
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
							return '-22%';
						}
						return '-22%';
					}
				}).show();
				$('#sql-preview').html(data.msg);
				$('#confirm-btn').unbind('click').click(function() {
					new FC.MessageLoader($('#result-info'), {
						msgClassName: 'alert-info',
						msg: '建表需要一定时间，请耐心等待！',
						keepOnly: true
					});
					window.location.href = window.location.href.replace(/#.*/gi, '') + '#top';
					$('#submit-btn').attr('disabled', 'disabled');
					$('#sql-modal').modal('hide');
					$.ajax({
						url: '/darkbat/json/createTableAction',
						type: 'post',
						dataType: 'json',
						data: {
							owner: userVerify.getPinyinName(),
							datasourceType: datasourceType,
							databaseName: databaseName,
							schemaName: schemaName,
							tableName: tableName,
							targetIsActiveSchedule: targetIsActiveScheduleVal,
							targetDatasourceType: targetDatasourceTypeVal,
							targetSchemaTable: targetSchemaTableVal,
							targetTableType: targetTableTypeVal,
							targetSegmentColumn: targetSegmentColumnVal
                            //targetWriteType : targetWriteType
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
							msg: '建表成功',
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
});

var targetIsActiveScheduleChange = function() {
	var targetIsActiveSchedule = $('#target-is-active-schedule');
	var targetDatasourceType = $('#target-datasource-type');
	var targetSchemaTable = $('#target-schema-table');
	var targetTableType = $('#target-table-type');
	var targetSegmentColumn = $('#target-segment-column');
	
	var targetIsActiveScheduleVal = targetIsActiveSchedule.val();
	
	targetDatasourceType.empty();
	targetSchemaTable.val('');
	targetTableType.empty();
	targetSegmentColumn.empty();

	if (targetIsActiveScheduleVal != '') {
		targetDatasourceType.append($('<option>').attr('value', '').text('---请选择---'));
		if (datasourceType == 'hive') {
			/*targetDatasourceType.append($('<option>').attr('value', 'greenplum').text('greenplum'));*/
            targetDatasourceType.append($('<option>').attr('value', 'gpreport').text('gp-report'));
            targetDatasourceType.append($('<option>').attr('value', 'gpanalysis').text('gp-analysis'));
		} else {
            targetDatasourceType.append($('<option>').attr('value', 'hive').text('hive'));
            /*
            if (datasourceType == 'mysql' && databaseName == 'DpDim'){
                targetDatasourceType.append($('<option>').attr('value', 'hive').text('hive'));
            } else{
                targetDatasourceType.append($('<option>').attr('value', 'greenplum').text('greenplum'));
                targetDatasourceType.append($('<option>').attr('value', 'hive').text('hive'));
            }
            */
		}
	}
};

var targetDatasourceTypeChange = function() {
	var targetIsActiveSchedule = $('#target-is-active-schedule');
	var targetDatasourceType = $('#target-datasource-type');
	var targetSchemaTable = $('#target-schema-table');
	var targetTableType = $('#target-table-type');

	var targetIsActiveScheduleVal = targetIsActiveSchedule.val();
	var targetDatasourceTypeVal = targetDatasourceType.val();
    //var targetWriteType = $('#target-write-type');

	targetSchemaTable.val('');
	var targetSegmentColumn = $('#target-segment-column');
	targetSegmentColumn.empty();

	if (targetDatasourceTypeVal == '') {
		targetTableType.empty();
	} else {
		if (targetDatasourceTypeVal == 'hive') {
			targetTableType.empty();
			if (targetIsActiveScheduleVal == '0') {
				targetTableType.append($('<option>').attr('value', '3').text('全量镜像表'));
				targetSegmentColumn.append($('<option>').attr('value', '').text('无分区字段'));
				if (datasourceType == 'greenplum') {
					targetSchemaTable.val('dptmp_' + tableNameSub);
				} else {
					targetSchemaTable.val('dptmp_' + tableNameLow);
				}
			} else if (targetIsActiveScheduleVal == '1') {
                if( databaseName == 'DpDim' && tableName.indexOf('dpdim_')==0){
                    targetTableType.append($('<option>').attr('value', '').text('---请选择---'));
                    targetTableType.append($('<option>').attr('value', '5').text('维度表'));
                } else if( databaseName == 'DpDim' && !tableName.indexOf('dpdim_')==0){
                    targetTableType.append($('<option>').attr('value', '').text('不符合规范'));
                } else{
                    targetTableType.append($('<option>').attr('value', '').text('---请选择---'));
                    targetTableType.append($('<option>').attr('value', '1').text('拉链表'));
                    targetTableType.append($('<option>').attr('value', '2').text('历史快照表'));
                    targetTableType.append($('<option>').attr('value', '4').text('日志表'));
                }
				targetSegmentColumn.append($('<option>').attr('value', '').text('无分区字段'));
			} else{

            }
		} else if (targetDatasourceTypeVal == 'greenplum') {
			targetTableType.empty();
			if (targetIsActiveScheduleVal == '0') {
				targetTableType.append($('<option>').attr('value', '3').text('全量镜像表'));
				targetSegmentColumn.append($('<option>').attr('value', '').text('无分区字段'));
				targetSchemaTable.val('dptmp.' + tableNameLow);
			} else if (targetIsActiveScheduleVal == '1') {
                if( datasourceType == 'hive' && tableName.indexOf("dpdim_") == 0){
                    targetTableType.append($('<option>').attr('value', '').text('---请选择---'));
                    targetTableType.append($('<option>').attr('value', '5').text('维度表'));
                } else{
                    targetTableType.append($('<option>').attr('value', '').text('---请选择---'));
                    targetTableType.append($('<option>').attr('value', '3').text('全量镜像表'));
                    targetTableType.append($('<option>').attr('value', '4').text('日志表'));
                    targetSegmentColumn.append($('<option>').attr('value', '').text('无分区字段'));
                }

				if (datasourceType == 'hive') {
					if (tableName.indexOf('dpdm_') == 0) {
						targetSchemaTable.val('dpdm.' + tableNameLow);
					} else if (tableName.indexOf('dpdw_') == 0) {
						targetSchemaTable.val('dpdw.' + tableNameLow);
					} else if (tableName.indexOf('dpfinance_') == 0) {
						targetSchemaTable.val('dpfinance.' + tableNameLow);
					} else if (tableName.indexOf('dpmid_') == 0) {
						targetSchemaTable.val('dpmid.' + tableNameLow);
					} else if (tableName.indexOf('dpods_') == 0) {
						targetSchemaTable.val('dpods.' + tableNameLow);
					} else if (tableName.indexOf('dpodssec_') == 0) {
						targetSchemaTable.val('dpodssec.' + tableNameLow);
					} else if (tableName.indexOf('dprpt_') == 0) {
					    targetSchemaTable.val('dprpt.' + tableNameLow);
					} else if (tableName.indexOf('dpdim_') == 0) {
                        targetSchemaTable.val('dpmid.' + tableNameLow);
					} else {
						targetSchemaTable.val('dptmp.' + tableNameLow);
					}
				} else {
					targetSchemaTable.val('dpods.' + tableNameLow);
				}
			}
		}else if (targetDatasourceTypeVal == 'gpreport') {
			targetTableType.empty();
			if (targetIsActiveScheduleVal == '1') {
                $.ajax({
                    url: '/darkbat/json/checkDBRule',
                    type: 'post',
                    dataType: 'json',
                    data: {
                        owner: userVerify.getPinyinName(),
                        databaseName: 'gp_report',
                        tableName: tableName,
                        schemaName: schemaName
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
                    } else{
                        if(data.msg == 1){
                            targetSchemaTable.val('bi.' + tableNameLow);
                            if( datasourceType == "hive" && tableName.indexOf("dpdim_") == 0){
                                targetTableType.append($('<option>').attr('value', '5').text('维度表'));
                            }else{
                                targetTableType.append($('<option>').attr('value', '').text('---请选择---'));
                                targetTableType.append($('<option>').attr('value', '3').text('全量镜像表'));
                                targetTableType.append($('<option>').attr('value', '4').text('日志表'));
                            }
                            targetSegmentColumn.append($('<option>').attr('value', '').text('无分区字段'));
                        }else{
                            new FC.MessageLoader($('#result-info'), {
                                msgClassName: 'alert-error',
                                msg: '表名称 '+tableName+' 不符合规则',
                                keepOnly: true
                            });
                            targetSegmentColumn.append($('<option>').attr('value', '').text('无分区字段'));
                        }
                    }
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
			}
		}
        else if (targetDatasourceTypeVal == 'gpanalysis') {
			targetTableType.empty();
			if (targetIsActiveScheduleVal == '1') {
                $.ajax({
                    url: '/darkbat/json/checkDBRule',
                    type: 'post',
                    dataType: 'json',
                    data: {
                        owner: userVerify.getPinyinName(),
                        databaseName: 'gp_analysis',
                        tableName: tableName,
                        schemaName: schemaName
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
                    } else{
                        if(data.msg == 1){
                            targetSchemaTable.val('bi.' + tableNameLow);
                            if( datasourceType == "hive" && tableName.indexOf("dpdim_") == 0){
                                targetTableType.append($('<option>').attr('value', '5').text('维度表'));
                            }else{
                                targetTableType.append($('<option>').attr('value', '3').text('全量镜像表'));
                                targetTableType.append($('<option>').attr('value', '4').text('日志表'));
                            }
                            targetSegmentColumn.append($('<option>').attr('value', '').text('无分区字段'));
                        }else{
                            new FC.MessageLoader($('#result-info'), {
                                msgClassName: 'alert-error',
                                msg: '表名称 '+tableName+' 不符合规则',
                                keepOnly: true
                            });
                            targetSegmentColumn.append($('<option>').attr('value', '').text('无分区字段'));
                        }
                    }
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
			}
		}
        else {
		}
	}
};

var targetTableTypeChange = function() {
	var targetIsActiveSchedule = $('#target-is-active-schedule');
	var targetDatasourceType = $('#target-datasource-type');
	var targetSchemaTable = $('#target-schema-table');
	var targetTableType = $('#target-table-type');

	var targetIsActiveScheduleVal = targetIsActiveSchedule.val();
	var targetDatasourceTypeVal = targetDatasourceType.val();
	var targetTableTypeVal = targetTableType.val();
	var targetSegmentColumn = $('#target-segment-column');
	targetSegmentColumn.empty();

	if (targetTableTypeVal == '') {
		targetSchemaTable.val('');
	} else {
		if (targetDatasourceTypeVal == 'hive') {
			if (targetIsActiveScheduleVal == '0') {
				if (datasourceType == 'greenplum') {
					targetSchemaTable.val('dptmp_' + tableNameSub);
				} else {
					targetSchemaTable.val('dptmp_' + tableNameLow);
				}
				targetSegmentColumn.append($('<option>').attr('value', '').text('无分区字段'));
			} else if (targetIsActiveScheduleVal == '1') {
				if (targetTableTypeVal == '1') {
					if (datasourceType == 'greenplum') {
						targetSchemaTable.val('load_' + tableNameSub);
					} else {
						targetSchemaTable.val('load_' + tableNameLow);
					}
				} else if(targetTableTypeVal == '5'){
					if (datasourceType == 'greenplum' ) {
                        if( tableName.indexOf('dpdim_') == 0 ){
                            targetSchemaTable.val(tableNameSub)
                        } else{
                            targetSchemaTable.val('dpdim_' + tableNameSub);
                        }
					} else {
                        if(tableNameLow.indexOf("dpdim_")==0){
                            targetSchemaTable.val(tableNameLow);
                        }else{
                            targetSchemaTable.val('dpdim_' + tableNameLow);
                        }
					}
				}else {
					if (datasourceType == 'greenplum') {
						targetSchemaTable.val('dpods_' + tableNameSub);
					} else {
						targetSchemaTable.val('dpods_' + tableNameLow);
					}
				}
				if (targetTableTypeVal == '1' || targetTableTypeVal == '3') {
					targetSegmentColumn.append($('<option>').attr('value', '').text('无分区字段'));
				} else if (targetTableTypeVal == '2') {
					targetSegmentColumn.append($('<option>').attr('value', 'hp_statdate').text('hp_statdate'));
				} else if (targetTableTypeVal == '5') {
					targetSegmentColumn.append($('<option>').attr('value', 'hp_cal_dt').text('hp_cal_dt'));
				} else if (targetTableTypeVal == '4') {
					$.each(columns, function() {
						targetSegmentColumn.append($('<option>').attr('value', this.columnName).text(this.columnName));
					});
				}
			}
		} else if (targetDatasourceTypeVal == 'greenplum') {
			if (targetIsActiveScheduleVal == '0') {
				if (datasourceType == 'greenplum') {
					targetSchemaTable.val('dptmp.' + tableNameSub);
				} else {
					targetSchemaTable.val('dptmp.' + tableNameLow);
				}
				targetSegmentColumn.append($('<option>').attr('value', '').text('无分区字段'));
			} else if (targetIsActiveScheduleVal == '1') {
				if (datasourceType == 'hive') {
					if (tableName.indexOf('dpdm_') == 0) {
						targetSchemaTable.val('dpdm.' + tableNameSub);
					} else if (tableName.indexOf('dpdw_') == 0) {
						targetSchemaTable.val('dpdw.' + tableNameSub);
					} else if (tableName.indexOf('dpfinance_') == 0) {
						targetSchemaTable.val('dpfinance.' + tableNameSub);
					} else if (tableName.indexOf('dpmid_') == 0) {
						targetSchemaTable.val('dpmid.' + tableNameSub);
					} else if (tableName.indexOf('dpods_') == 0) {
						targetSchemaTable.val('dpods.' + tableNameSub);
					} else if (tableName.indexOf('dpodssec_') == 0) {
						targetSchemaTable.val('dpodssec.' + tableNameSub);
					} else if (tableName.indexOf('dprpt_') == 0) {
						targetSchemaTable.val('dprpt.' + tableNameSub);
					} else if (tableName.indexOf('dpdim_') == 0) {
                        targetSchemaTable.val('dpdim.' + tableNameLow);
					} else {
						targetSchemaTable.val('');
					}
				} else if(targetTableTypeVal == '5'){ 
					    targetSchemaTable.val('dpdim.' + tableNameLow);
				}
				else {
					targetSchemaTable.val('dpods.' + tableNameLow);
				}
				if (targetTableTypeVal == '3' || targetTableTypeVal == '5') {
					targetSegmentColumn.append($('<option>').attr('value', '').text('无分区字段'));
				} else if (targetTableTypeVal == '4') {
					var flag = 0;
					$.each(columns, function() {
						if (datasourceType == 'hive') {
							flag = 1;
							targetSegmentColumn.append($('<option>').attr('value', this.columnName).text(this.columnName));
						} else {
							if (this.columnType == 'datetime' || this.columnType == 'smalldatetime' || this.columnType == 'date' || this.columnType == 'timestamp' || this.columnType == 'time') {
								flag = 1;
								targetSegmentColumn.append($('<option>').attr('value', this.columnName).text(this.columnName));
							}
						}
					});
					if (flag == 0) {
						targetSegmentColumn.append($('<option>').attr('value', '').text('无分区字段'));
					}
				}
			}
		} else if (targetDatasourceTypeVal == 'gpreport') {
			 if (targetIsActiveScheduleVal == '1') {
				if (datasourceType == "hive" && (targetTableTypeVal == '3' || targetTableTypeVal == '5')) {
					targetSegmentColumn.append($('<option>').attr('value', '').text('无分区字段'));
				} else if (datasourceType == "hive" && targetTableTypeVal == '4') {
					var flag = 0;
					$.each(columns, function() {
						if (datasourceType == 'hive') {
							flag = 1;
							targetSegmentColumn.append($('<option>').attr('value', this.columnName).text(this.columnName));
						} else {
							if (this.columnType == 'datetime' || this.columnType == 'smalldatetime' || this.columnType == 'date' || this.columnType == 'timestamp' || this.columnType == 'time') {
								flag = 1;
								targetSegmentColumn.append($('<option>').attr('value', this.columnName).text(this.columnName));
							}
						}
					});
					if (flag == 0) {
						targetSegmentColumn.append($('<option>').attr('value', '').text('无分区字段'));
					}
				}
			}
		}
        else if (targetDatasourceTypeVal == 'gpanalysis') {
			 if (targetIsActiveScheduleVal == '1') {
				if (datasourceType == "hive" && (targetTableTypeVal == '3' || targetTableTypeVal == '5')) {
					targetSegmentColumn.append($('<option>').attr('value', '').text('无分区字段'));
				} else if (datasourceType == "hive" && targetTableTypeVal == '4') {
					var flag = 0;
					$.each(columns, function() {
						if (datasourceType == 'hive') {
							flag = 1;
							targetSegmentColumn.append($('<option>').attr('value', this.columnName).text(this.columnName));
						} else {
							if (this.columnType == 'datetime' || this.columnType == 'smalldatetime' || this.columnType == 'date' || this.columnType == 'timestamp' || this.columnType == 'time') {
								flag = 1;
								targetSegmentColumn.append($('<option>').attr('value', this.columnName).text(this.columnName));
							}
						}
					});
					if (flag == 0) {
						targetSegmentColumn.append($('<option>').attr('value', '').text('无分区字段'));
					}
				}
			}
		} else {
		}
	}
};

