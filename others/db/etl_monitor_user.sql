CREATE TABLE `etl_monitor_user` (
   `auto_id` INT(11) NOT NULL AUTO_INCREMENT COMMENT '自增ID',
   `user_name` VARCHAR(255) NOT NULL COMMENT '值班人员',
   `mobile_no` VARCHAR(255) NOT NULL COMMENT '值班手机号',
   `office_no` VARCHAR(255) DEFAULT NULL COMMENT '分机号',
   `begin_date` VARCHAR(255) DEFAULT NULL COMMENT '值班开始日期',
   `add_user` VARCHAR(255) NOT NULL,
   `add_time` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
   `update_user` VARCHAR(255) NOT NULL,
   `update_time` TIMESTAMP NOT NULL DEFAULT '0000-00-00 00:00:00',
   PRIMARY KEY (`auto_id`)
 ) ENGINE=INNODB DEFAULT CHARSET=utf8;
 
insert into etl_monitor_user (user_name, mobile_no, office_no, add_user, update_user, update_time, begin_date) values  ('张翼' , '18616363260', '1726', '凤成', '凤成', '2013-12-27 16:34:23', '2013-12-21');
insert into etl_monitor_user (user_name, mobile_no, office_no, add_user, update_user, update_time) values  ('孟涛' , '18502147334', '0', '凤成', '凤成', '2013-12-27 16:34:23');
insert into etl_monitor_user (user_name, mobile_no, office_no, add_user, update_user, update_time) values  ('金澜涛', '5921778090', '0', '凤成', '凤成', '2013-12-27 16:34:23');
insert into etl_monitor_user (user_name, mobile_no, office_no, add_user, update_user, update_time) values  ('房明' , '15021313607', '1811', '凤成', '凤成', '2013-12-27 16:34:23');
insert into etl_monitor_user (user_name, mobile_no, office_no, add_user, update_user, update_time) values  ('朱广彬', '5721141314', '0', '凤成', '凤成', '2013-12-27 16:34:23');
insert into etl_monitor_user (user_name, mobile_no, office_no, add_user, update_user, update_time) values  ('陈昱康', '5901746211', '1809', '凤成', '凤成', '2013-12-27 16:34:23');
insert into etl_monitor_user (user_name, mobile_no, office_no, add_user, update_user, update_time) values  ('樊聪', '5900670896', '0', '凤成', '凤成', '2013-12-27 16:34:23');
insert into etl_monitor_user (user_name, mobile_no, office_no, add_user, update_user, update_time) values  ('王冰', '3564589482', '1739', '凤成', '凤成', '2013-12-27 16:34:23');
insert into etl_monitor_user (user_name, mobile_no, office_no, add_user, update_user, update_time) values  ('唐弘頔', '8017404270', '1852', '凤成', '凤成', '2013-12-27 16:34:23');
insert into etl_monitor_user (user_name, mobile_no, office_no, add_user, update_user, update_time) values  ('周晓敏', '3611873561', '1872', '凤成', '凤成', '2013-12-27 16:34:23');
insert into etl_monitor_user (user_name, mobile_no, office_no, add_user, update_user, update_time) values  ('陈地树', '8001989029', '1873', '凤成', '凤成', '2013-12-27 16:34:23');
insert into etl_monitor_user (user_name, mobile_no, office_no, add_user, update_user, update_time) values  ('赵宏', '3817799296', '1777', '凤成', '凤成', '2013-12-27 16:34:23');
insert into etl_monitor_user (user_name, mobile_no, office_no, add_user, update_user, update_time) values  ('张磊.s', '3916425967', '1826', '凤成', '凤成', '2013-12-27 16:34:23');
insert into etl_monitor_user (user_name, mobile_no, office_no, add_user, update_user, update_time) values  ('曹一帆', '5800922415', '1855', '凤成', '凤成', '2013-12-27 16:34:23');
insert into etl_monitor_user (user_name, mobile_no, office_no, add_user, update_user, update_time) values  ('韩钊', '3761109768', '1835', '凤成', '凤成', '2013-12-27 16:34:23');
insert into etl_monitor_user (user_name, mobile_no, office_no, add_user, update_user, update_time) values  ('金山山', '3107721356', '1865', '凤成', '凤成', '2013-12-27 16:34:23');
insert into etl_monitor_user (user_name, mobile_no, office_no, add_user, update_user, update_time) values  ('陈小梦', '8658882986', '1838', '凤成', '凤成', '2013-12-27 16:34:23');
insert into etl_monitor_user (user_name, mobile_no, office_no, add_user, update_user, update_time) values  ('凤成', '3661895847', '1870', '凤成', '凤成', '2013-12-27 16:34:23');
insert into etl_monitor_user (user_name, mobile_no, office_no, add_user, update_user, update_time) values  ('陆刚', '3601992651', '1885', '凤成', '凤成', '2013-12-27 16:34:23');
insert into etl_monitor_user (user_name, mobile_no, office_no, add_user, update_user, update_time) values  ('姚宇捷', '8621990199', '1900', '凤成', '凤成', '2013-12-27 16:34:23');
insert into etl_monitor_user (user_name, mobile_no, office_no, add_user, update_user, update_time) values  ('王晓宁', '3636412265', '1937', '凤成', '凤成', '2013-12-27 16:34:23');
insert into etl_monitor_user (user_name, mobile_no, office_no, add_user, update_user, update_time) values  ('陈雄', '8668199918', '1930', '凤成', '凤成', '2013-12-27 16:34:23');
insert into etl_monitor_user (user_name, mobile_no, office_no, add_user, update_user, update_time) values  ('许景楠', '5601622488', '1957', '凤成', '凤成', '2013-12-27 16:34:23');
insert into etl_monitor_user (user_name, mobile_no, office_no, add_user, update_user, update_time) values  ('吴经', '3764540102', '1975', '凤成', '凤成', '2013-12-27 16:34:23');
insert into etl_monitor_user (user_name, mobile_no, office_no, add_user, update_user, update_time) values  ('周希', '3764055517', '1967', '凤成', '凤成', '2013-12-27 16:34:23');
insert into etl_monitor_user (user_name, mobile_no, office_no, add_user, update_user, update_time) values  ('邹玉静', '5618153177', '0', '凤成', '凤成', '2013-12-27 16:34:23');
insert into etl_monitor_user (user_name, mobile_no, office_no, add_user, update_user, update_time) values  ('岳小宁', '8501702883', '1993', '凤成', '凤成', '2013-12-27 16:34:23');
insert into etl_monitor_user (user_name, mobile_no, office_no, add_user, update_user, update_time) values  ('年国栋', '8516180357', '1991', '凤成', '凤成', '2013-12-27 16:34:23');
insert into etl_monitor_user (user_name, mobile_no, office_no, add_user, update_user, update_time) values  ('赵昊宇', '8502116904', '0', '凤成', '凤成', '2013-12-27 16:34:23');
insert into etl_monitor_user (user_name, mobile_no, office_no, add_user, update_user, update_time) values  ('张超.sh2', '3851649582', '1717', '凤成', '凤成', '2013-12-27 16:34:23');

ALTER TABLE `etl_monitor_user` 
   ADD COLUMN `order_id` INT(11) NOT NULL COMMENT '值班顺序: 升序' AFTER `office_no`