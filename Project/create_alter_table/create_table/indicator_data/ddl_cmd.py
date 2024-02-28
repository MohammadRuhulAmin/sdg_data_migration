create_indicator_data = """
CREATE TABLE `indicator_data` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `ind_id` int(11) DEFAULT '0',
  `ind_def_id` int(11) DEFAULT '0',
  `source_id` int(11) DEFAULT '0',
  `provider_id` int(11) DEFAULT '0',
  `approver_id` int(11) DEFAULT '0',
  `publisher_id` int(11) DEFAULT '0',
  `sent_for_approval_date` datetime DEFAULT NULL,
  `approved_date` datetime DEFAULT NULL,
  `published_date` datetime DEFAULT NULL,
  `data_period` varchar(255) COLLATE utf8_unicode_ci DEFAULT NULL,
  `raw_data_json` json DEFAULT NULL,
  `geo_data_json` json DEFAULT NULL,
  `data_value` double DEFAULT '0' COMMENT '-1 = Fully meets requirements, -2 = Approaches requirements, -3 = Does not meet requirements',
  `remarks` varchar(255) COLLATE utf8_unicode_ci DEFAULT NULL,
  `national_metadata_file` varchar(255) COLLATE utf8_unicode_ci DEFAULT NULL,
  `status` tinyint(4) DEFAULT '1' COMMENT '1=created, 2=sent for approval, 3=approved, 4=published',
  `is_archived` tinyint(4) DEFAULT '0',
  `created_at` datetime DEFAULT NULL,
  `created_by` int(11) DEFAULT '0',
  `updated_at` datetime DEFAULT NULL,
  `updated_by` int(11) DEFAULT '0',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=4 DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci


"""