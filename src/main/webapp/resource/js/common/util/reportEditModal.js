define(function(require) {
    require('jquery');

    var nodes = null, selectInfo = '<select name="" id="" data-key="timeRange" class="input-small">'
            + '      <option value="1">1</option>'
            + '      <option value="2">2</option>'
            + '      <option value="3">3</option>'
            + '      <option value="4">4</option>'
            + '      <option value="5">5</option>'
            + '      <option value="6">6</option>'
            + '      <option value="7" selected>7</option>'
            + '      <option value="8">8</option>'
            + '      <option value="9">9</option>'
            + '      <option value="10">10</option>'
            + '      <option value="11">11</option>'
            + '      <option value="12">12</option>'
            + '      <option value="13">13</option>'
            + '      <option value="14">14</option>'
            + '      <option value="15">15</option>'
            + '      <option value="16">16</option>'
            + '      <option value="17">17</option>'
            + '      <option value="18">18</option>'
            + '      <option value="19">19</option>'
            + '      <option value="20">20</option>'
            + '      <option value="21">21</option>'
            + '      <option value="22">22</option>'
            + '      <option value="23">23</option>'
            + '      <option value="24">24</option>'
            + '      <option value="25">25</option>'
            + '      <option value="26">26</option>'
            + '      <option value="27">27</option>'
            + '      <option value="28">28</option>'
            + '      <option value="29">29</option>'
            + '      <option value="30">30</option>'
            + '      <option value="31">31</option>' + '</select>';

    function _initReportEdit(data) {
        $div = $('#report-chooser-edit');
        var initHtml = ''
                + '<div class="control-group row-fluid" style="margin-left: 160px;">'
                + '<div class="controls span8">'
                + '<table class="table-bordered table" style="width: 500px;"><thead><tr><th>报表名称</th><th>数据周期</th><th>操作</th></tr></thead>'
                + '<tbody></tbody></table>' + '</div>' + '</div>';
        $div.html(initHtml);

        $(data).each(function(i, e) {
            _addReportEdit(e);
        });
    }

    function _addReportEdit(data) {
        $('#page-item-chooser-edit').addClass('hide');
        var $pDiv = $('#report-chooser-edit'), $div = $pDiv.find('tbody');

        $('#page-item-chooser-edit').addClass('hide');
        if ($div.length > 0) {
            var initOne = '<tr>';
            initOne += '<td class="report-name" report-id=' + data.reportId
                    + '>' + data.reportName + '</td>';

            var dataCycle = 0;
            var isEdit = 0;
            if (data.dataCycle != null) {
                dataCycle = data.dataCycle;
            }
            $(data.vnQuery.vnXQueryParamList).each(
                    function(i, e) {
                        var vnParam = e.vnParam;
                        if (vnParam.paramType == 'CASCADE') {
                            if (vnParam.paramSubtype == 'DATE'
                                    || vnParam.paramSubtype == 'MONTH'
                                    || vnParam.paramSubtype == 'YEAR') {
                                if (data.dataCycle == null) {
                                    dataCycle = 7;
                                }
                                isEdit = 1;
                                return false;
                            }
                        }
                        if (vnParam.paramType == 'CALENDAR') {
                            if (data.dataCycle == null) {
                                dataCycle = 1;
                            }
                        }
                    });

            if (dataCycle == 0) {
                initOne += '<td class="report-cycle" >1</td>';
            } else if (isEdit == 0) {
                initOne += '<td class="report-cycle" >' + dataCycle + '</td>';
            } else {
                initOne += '<td class="report-cycle" >' + selectInfo + '</td>';
            }
            initOne += '<td><a class="btn btn-delete btn-info">删除</a></td>';
            initOne += '</tr>';
            $div.append(initOne);
            if (isEdit == 1) {
                $('#report-chooser-edit tbody tr:last select').val(dataCycle);
            }
        } else {
            _initReportEdit([ data ]);
        }
        $pDiv.removeClass('hide');
        $div.find('.btn-delete').unbind();
        $div.find('.btn-delete').on('click', _delReportEdit);
    }

    function _delReportEdit(e) {
        var reportId = parseInt($(this).parent().parent()
                .find('td.report-name').attr("report-id"));
        if (reportId) {
            var node = nodes[reportId.toString()];
            $('#' + node.tId).find('span.chk').click();
        }
    }

    function _disappearReportEdit() {
        var data = _getReportEdit();
        if (data.length == 0) {
            $('#report-chooser-edit').html('');
        }
    }

    function _getReportEdit() {
        var data = [];
        $('#report-chooser-edit tbody tr').each(
                function(i, e) {
                    data.push({
                        id : $(e).find('.report-name').attr('report-id'),
                        name : $(e).find('.report-name').text(),
                        type: 'REPORT',
                        cycle : $(e).find('select').val() ? $(e).find('select')
                                .val()
                                : ($(e).find('.report-cycle').text() == '无' ? 0
                                        : 1),
                        displayIndex: 0,
                        isHide: 0
                    });
                });
        return data;
    }

    return {
        setOptions : function(options) {
            if (options.nodes)
                nodes = options.nodes;
        },
        initReportEdit : function(data) {
            _initReportEdit(data);
        },
        addReportEdit : function(data) {
            _addReportEdit(data);
        },
        formatReportEdit : function(data) {
            var report = {
                reportId : data.reportId,
                reportName : data.reportName,
                reportCycle : 0
            };
            //            $(data.vnQuery.vnXQueryParamList).each(function(i,e){
            //                
            //            });
            return report;
        },
        delReportEdit : function(data) {
            $('#report-chooser-edit td[report-id=' + data.reportId + ']')
                    .parent().remove();
            _disappearReportEdit();
        },
        getReportEdit : function() {
            return _getReportEdit();
        }
    };
});