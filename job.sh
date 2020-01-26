sqoop import --bindir /usr/lib/sqoop --connect jdbc:mysql://localhost:3306/sqoopex --username root --password-file /root/etl-offload/sqoop.password  --split-by data_value --columns series_reference,period,data_value,status,units,magnitude,subject,groups,series_title_1,series_title_2,series_title_3,series_title_4,series_title_5 --table local_finance --delete-target-dir --target-dir /root/sqoopex_import/local_data --fields-terminated-by "," --hive-import --hive-overwrite --hive-table etloffload.local_finance





sqoop import --bindir /usr/lib/sqoop --connect jdbc:mysql://localhost:3306/sqoopex --username root --password-file /root/etl-offload/sqoop.password  --split-by data_value --columns series_reference,period,data_value,status,units,magnitude,subject,groups,series_title_1,series_title_2,series_title_3,series_title_4,series_title_5 --table central_finance --delete-target-dir --target-dir /root/sqoopex_import/central_data --fields-terminated-by "," --hive-import --hive-overwrite --hive-table etloffload.central_finance




sqoop import --bindir /usr/lib/sqoop --connect jdbc:mysql://localhost:3306/sqoopex --username root --password-file /root-offload/sqoop.password  --split-by data_value --columns series_reference,period,data_value,status,units,magnitude,subject,groups,series_title_1,series_title_2,series_title_3,series_title_4,series_title_5 --table general_finance --delete-target-dir --target-dir /root.sqoopex_import/general_data --fields-terminated-by "," --hive-import --hive-overwrite --hive-table etloffload.general_finance


spark-submit 

/root/etl-offload/py_spark.py

