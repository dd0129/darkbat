define(function(require) {
    var comm = require('common'),
        modalUtil= require('localJs/common/util/modalWithId');
    
    var ResourceTreeUtil = (function($, comm) {
        var _isOnline = comm.isOnline, 
            _resourcePath = _isOnline ? 'http://venus.data.dp/rest/config/resource'
                : '/venus/rest/config/resource',
            _folderPath = _isOnline ? 'http://venus.data.dp/rest/config/folder'
                        : '/venus/rest/config/folder',
            _tokenPath = '',
            _token = '',
//          _enableEdit,
            _zTree,
            _zTreeCount = 0;

        
        var _resourceType= {
                DATASOURCE : 'DATASOURCE',
                QUERY : 'QUERY',
                PARAM : 'PARAM',
                REPORT : 'REPORT',
                DASHBOARD : 'DASHBOARD',
                PAGE : 'PAGE',
                FOLDER : 'FOLDER',
                ALL : 'ALL',
                OTHER : 'OTHER'
            };
        
        function _getReourceList(rootId, options) {
            $.ajax({
                url : _resourcePath + '/' + rootId + _tokenPath,
                type : 'GET',
                dataType : 'json'
            })
            .done(options.onSuccess)
            .fail(options.onError);
        }
        
        function _editFolder(json, options) {
            $.ajax({
                url : _folderPath+'/'+json.folderId + _tokenPath,
                type : 'PUT',
                data: json,
                dataType : 'json'
            })
            .done(options.onSuccess)
            .fail(options.onError);
        }
        
        function _renderResourceTree(rootId, resourceType, zTreeContainerId, optionsZTreeId, optionsOnClick, enableEdit) {
            $(zTreeContainerId).empty();
            var zTreeId = optionsZTreeId ? optionsZTreeId : "ztree-resource-list-"+_zTreeCount;
            _zTreeCount++;
            
            $(zTreeContainerId).append('<ul id="'+zTreeId+'" class="ztree"></ul>');
            
            if(enableEdit){
                jQuery("head").append("<style>.ztree li span.button.add {margin-left:2px; margin-right: -1px; vertical-align:top; *vertical-align:middle;background-image: url(http://www.ztree.me/v3/css/zTreeStyle/img/zTreeStandard.png);background-position: -144px 0px;background-repeat: no-repeat;}</style>");
            };
            
            function addHoverDom(treeId, treeNode) {
                var sObj = $("#" + treeNode.tId + "_span");
                if (treeNode.editNameFlag || $("#addBtn_"+treeNode.tId).length>0) return;
                var addStr = "<span class='button add' id='addBtn_" + treeNode.tId
                    + "' title='添加' onfocus='this.blur();'></span>";
                sObj.after(addStr);
                var btn = $("#addBtn_"+treeNode.tId);
                if (btn) btn.bind("click", function(){
                    
                    var folderName;
//                  seajs.use(['localJs/common/util/modalWithId'], function(modalUtil) {});

                    $pDiv=modalUtil.showMessage({
                        message: '请输入目录名称',
                        modalId:'set-folder-name',
                        innerHTML: ''+
                        '<table class="table table-bordered">'+
                            '<tbody>'+
                                '<tr>'+
                                    '<td>目录名称</td>'+
                                    '<td><input type="text" id="folder-name" class="input-xlarge" placeholder="请输入目录名称..." data-key="folderName" required=""></td>'+
                                '</tr>'+
                            '</tbody>'+
                        '</table>',
                        hasSubmit: true,
                        onSubmit: function(e) {
                            folderName = $('#folder-name').val();
                            $.ajax({
                            url : _folderPath +'/' + _tokenPath,
                            type : 'POST',
                            data: {
                                'pid':treeNode.id,
                                'folderName':folderName
                            },
                            dataType : 'json'
                            })
                            .done(function(cb){
                                 _getReourceList(treeNode.id, {
                                     onSuccess:function(cb){
                                         _zTree.removeChildNodes(treeNode);
                                         var nodes = _resourcesToZTreeNodes(cb,resourceType);
                                         for(var i in nodes){
                                             if(i>0){
                                                 if(resourceType===_resourceType.FOLDER){
                                                     _zTree.addNodes(_zTree.getNodeByParam('id',nodes[i].pId),{id:(nodes[i].id), pId:nodes[i].pId,name:nodes[i].name,isParent:true, resourceId:nodes[i].resourceId})
                                                 }else{
                                                     _zTree.addNodes(_zTree.getNodeByParam('id',nodes[i].pId),{id:(nodes[i].id), pId:nodes[i].pId,name:nodes[i].name,resourceId:nodes[i].resourceId})
                                                 }
                                             }
                                         }
                                        _zTree.expandAll(true);
                                     },
                                     onError:function(cb){
                                         console.log('获取目录失败');
                                     }
                                 })
                            })
                            .fail(function(){
                                console.log('添加目录失败');
                            });
                            $pDiv.modal('hide');
                            setTimeout(function() {
                                $pDiv.remove();
                            }, 1000);
//                          return true;
                        }
                    });
                     modalUtil.fixCss($pDiv, {
                            'width':  '560px',
                            'left': '50%',
                            'margin-left': '-420px'
                        });
                });
            };
            
            function removeHoverDom(treeId, treeNode) {
                $("#addBtn_"+treeNode.tId).unbind().remove();
            };
            
            seajs.use([ 'zTree' ], function(tree) {
                _getReourceList(rootId, {
                    onSuccess : function(cb) {
                        var nodes = _resourcesToZTreeNodes(cb,resourceType);
                        var treeSetting = {
                            treeId : 'tree-folder',
                            view : {
                                addHoverDom: enableEdit?addHoverDom:null,
                                removeHoverDom:enableEdit? removeHoverDom:null,
                                selectedMulti : false
                            },
                            data : {
                                keep:{
                                    parent:true
                                },
                                simpleData : {
                                    enable : true
                                }
                            },
                            edit:{
                                editNameSelectAll: true,
                                enable: enableEdit,
                                showRemoveBtn: false,
                                showRenameBtn : true,
                                renameTitle : "重命名"
                            },
                            callback : {
                                onClick : optionsOnClick,
                                onRename:function(event, treeId, treeNode, isCancel){
                                    _editFolder({
                                        'pid':treeNode.pId,
                                        'folderId':treeNode.resourceId,
                                        'folderName':treeNode.name
                                    },{});
                                }
                            }
                        };
                        _zTree= $.fn.zTree.init($('#'+zTreeId).html(''),
                                treeSetting, nodes);
                        _zTree.expandAll(true);
                    },
                    onError:function(cb){
                        console.log('获取资源列表失败');
                    }
                });
            });
        }

        /**
         * 将后台的节点转换为ztree需要的节点
         * @param  Object resourceNode [description]
         * @param  String type         资源类型
         * @return Array              [description]
         */
        function _resourcesToZTreeNodes(resourceNode, type) {
            var ret = [];
            var nodes = resourceNode.children;

            var pushNode = function(ret, fromNode) {
                var resourceType = fromNode.resourceType;
          
                if(resourceType === _resourceType.FOLDER && _isEmptyFolder(fromNode)) {
                    //Do Nothing
                } else {
                    if (
                        resourceType === type || 
                        resourceType === _resourceType.FOLDER ||
                        resourceType === _resourceType.ALL ) {
                        
                        var node = {};
                        node.id = fromNode.autoId;
                        node.pId = fromNode.resourcePid;
                        node.name = fromNode.resourceName;
                        node.type = resourceType;
                        node.resourceId = fromNode.resourceId;
//                        node.resourceConfig = fromNode.resourceConfig ? $.parseJSON(fromNode.resourceConfig) : null;
                        node.resourceConfig = fromNode.resourceConfig;
                        
                        if(node.type === _resourceType.FOLDER) {
                            node.isParent = true;
                        }
                        ret.push(node);
                    }
                }
                return ret;
            };

            ret = pushNode(ret, resourceNode);

            while (nodes && nodes.length) {
                var current = nodes.splice(0, 1)[0];

                ret = pushNode(ret, current);
                nodes = nodes.concat(current.children || []);
            }

            return ret;
        }

        function _isEmptyFolder(resourceNode) {
            var isEmpty = true,
                node = FC.cloneObject(resourceNode),
                kids = node.children,
                currentNode;

            while(kids && kids.length) {
                currentNode = kids.splice(0, 1)[0];
                if(currentNode.resourceType !== _resourceType.FOLDER) {
                    isEmpty = false;
                    break;
                }

                kids = kids.concat(currentNode.children || []);
            }
            return isEmpty;
        }
        
        return {
            /**
             * options:     {
             *                  token:
             *                  enableEdit: true|false
             *                  defaultFolder:
             *                  resourceType:
             *                  zTreeContainerId:
             *                  zTreeId:
             *                  onClick:
             *              }
             */
            init: function(options) {
                _token = options.token;
                _tokenPath = '?token=' + options.token;
                var enableEdit = options.resourceType === _resourceType.FOLDER ? options.enableEdit : false;
                _renderResourceTree(
                        options.defaultFolder,
                        options.resourceType,
                        options.zTreeContainerId,
                        options.zTreeId,
                        options.onClick, 
                        enableEdit);
            },
            defaultFolder: {
                ROOT : 1,
                DATASOURCE : 2,
                QUERY : 3,
                PARAM : 4,
                REPORT : 5,
                DASHBOARD : 6,
                PAGE : 7
            },
            resourceType :_resourceType,
            getChooseTreeNode:  function() {
                return  _zTree.getSelectedNodes()[0];
            },
            
            /**
             * 将后台的节点转换为ztree需要的节点
             * @param  Object resourceNode [description]
             * @param  String type         资源类型
             * @return Array              [description]
             */
            resourcesToZTreeNodes: _resourcesToZTreeNodes
        };

    })($, comm);

    return ResourceTreeUtil;
});