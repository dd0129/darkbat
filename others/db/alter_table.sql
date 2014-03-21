alter table `DianPingDW`.`etl_mail_info` 
   add column `item_type` varchar(11) DEFAULT 'REPORT' NOT NULL COMMENT 'REPORT|PAGE' after `item_id_list`
   
alter table `DianPingDW`.`etl_mail_detail` 
   add column `item_type` varchar(11) DEFAULT 'REPORT' NOT NULL COMMENT 'REPORT|TABLE|CHART' after `report_id`
   
alter table `DianPingDW`.`etl_mail_detail` 
   add column `display_index` int(11) DEFAULT 0 NOT NULL COMMENT '展示顺序' after `data_cycle`
   
alter table `DianPingDW`.`etl_mail_detail` 
   add column `is_hide` int(11) DEFAULT 0 NOT NULL COMMENT '是否隐藏' after `display_index`