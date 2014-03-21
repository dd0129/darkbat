define(function(require) {

	return {
        /**
         * 从数组删除一个元素
         * @param  {[type]} arr [description]
         * @param  {[type]} ele [description]
         * @return {[type]}     [description]
         */
        deleteOneEleFromArr: function(arr, ele) {
            for (var i = 0; i < arr.length; i++) {
                if(arr[i] === ele) {
                    arr.splice(i, 1);
                    break;
                }
            }

            return arr;
        },

        clearModal: function(modalId) {
            var $modal = $('#' + modalId);

            if($modal.length) {
                $modal.modal('hide');
                $modal.remove();
            }
        },

        /**
         * 判断数组是否含有元素
         * @param  {[type]} arr [description]
         * @param  {[type]} ele [description]
         * @return {[type]}     [description]
         */
        hasElementInArray: function(arr, ele) {
            var isExist = false;

            for (var i = 0; i < arr.length; i++) {
                if(arr[i] === ele) {
                    isExist = true;
                    break;
                }
            }

            return isExist;
        },

        /**
         * 构造表id -> 节点的字典信息
         * @param  {[type]} nodes [description]
         * @return {[type]}       [description]
         */
        getRptToNodeMap: function(nodes) {
            var ret = {};
            $(nodes).each(function(i, e) {
                if(!e.isParent) {
                    ret[e.resourceId.toString()] = e;
                }
            });

            return ret;
        },

		isOnline: window.location.host === 'data.dp'
	};
});