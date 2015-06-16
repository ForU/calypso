DROP TABLE IF EXISTS `person`;
CREATE TABLE `person` (
`id` int(10) unsigned NOT NULL AUTO_INCREMENT,
`name` varchar(127) NOT NULL DEFAULT '' comment '',
`sex` enum('FEMALE','MALE', 'UNKNOWN') NOT NULL DEFAULT 'UNKNOWN' comment '',
`age` int(10) unsigned NOT NULL DEFAULT 0 comment '',
PRIMARY KEY (`id`),
UNIQUE KEY `name` (`name`)
) ENGINE=InnoDB AUTO_INCREMENT=74 DEFAULT CHARSET=utf8;


DROP TABLE IF EXISTS `orders`;
CREATE TABLE `orders` (
`id` int(10) unsigned NOT NULL AUTO_INCREMENT,
`serial` varchar(127) NOT NULL DEFAULT '' comment '',
`person_id` int(10) unsigned NOT NULL DEFAULT 0 comment '',
`create_time` datetime NOT NULL DEFAULT 0 COMMENT 'time of creation',
PRIMARY KEY (`id`),
UNIQUE KEY `name` (`serial`)
) ENGINE=InnoDB AUTO_INCREMENT=74 DEFAULT CHARSET=utf8;
