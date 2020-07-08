package com.qianfeng.util

import java.sql.Connection
import java.util.Properties

import com.alibaba.druid.pool.DruidDataSourceFactory

object DruidUtil {
	def getdbConnection(): Connection ={
		val properties = new Properties()
		properties.load(DruidUtil.getClass.getClassLoader.getResourceAsStream("db.properties"))
		val dataSource = DruidDataSourceFactory.createDataSource(properties)
		val connection = dataSource.getConnection()
		connection
	}
}
