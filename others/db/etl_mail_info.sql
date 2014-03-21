SET NAMES utf8;
SET FOREIGN_KEY_CHECKS = 0;

-- ----------------------------
--  Table structure for `etl_mail_info`
-- ----------------------------
DROP TABLE IF EXISTS `etl_mail_info`;
CREATE TABLE `etl_mail_info` (
  `mail_id` int(11) NOT NULL AUTO_INCREMENT,
  `mail_title` varchar(300) NOT NULL,
  `send_cycle` varchar(1) NOT NULL,
  `mail_content` text,
  `user_email_list` text,
  `item_id_list` varchar(200) NOT NULL,
  `system_id` int(10) NOT NULL,
  `add_user` varchar(100) NOT NULL,
  `update_user` varchar(100) NOT NULL,
  `add_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `update_time` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',
  PRIMARY KEY (`mail_id`)
) ENGINE=InnoDB AUTO_INCREMENT=45 DEFAULT CHARSET=utf8;

ALTER TABLE `DianPingDW2`.`etl_mail_info` ADD COLUMN `send_time` varchar(5) NOT NULL DEFAULT '10:00' AFTER `update_time`;

ALTER TABLE `DianPingDW2`.`etl_mail_info` CHANGE COLUMN `send_time` `send_time` varchar(5) NOT NULL DEFAULT '10:00' AFTER `system_id`, CHANGE COLUMN `add_user` `add_user` varchar(100) NOT NULL COMMENT '添加人' AFTER `send_time`, CHANGE COLUMN `update_user` `update_user` varchar(100) NOT NULL COMMENT '更新人' AFTER `add_user`, CHANGE COLUMN `add_time` `add_time` timestamp NOT NULL ON UPDATE CURRENT_TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '添加时间' AFTER `update_user`, CHANGE COLUMN `update_time` `update_time` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00' COMMENT '更新时间' AFTER `add_time`;

ALTER TABLE `DianPingDW2`.`etl_mail_info` ADD COLUMN `time_range` int(1) AFTER `system_id`, CHANGE COLUMN `add_user` `add_user` varchar(100) NOT NULL COMMENT '添加人' AFTER `time_range`, CHANGE COLUMN `update_user` `update_user` varchar(100) NOT NULL COMMENT '更新人' AFTER `add_user`, CHANGE COLUMN `add_time` `add_time` timestamp NOT NULL ON UPDATE CURRENT_TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '添加时间' AFTER `update_user`, CHANGE COLUMN `update_time` `update_time` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00' COMMENT '更新时间' AFTER `add_time`, CHANGE COLUMN `send_time` `send_time` varchar(5) NOT NULL DEFAULT '10:00' AFTER `update_time`;

ALTER TABLE `DianPingDW2`.`etl_mail_info` ADD COLUMN `is_valid` int(1) NOT NULL DEFAULT '0' COMMENT '是否生效 0: 否  1: 是' AFTER `send_time`;

ALTER TABLE `DianPingDW2`.`etl_mail_info` ADD COLUMN `task_id` int(11) COMMENT '邮件任务ID' AFTER `is_valid`;