sqoop import --connect jdbc:mysql://cxln2.c.thelab-240901.internal/sqoopex --username sqoopuser --password-file /user/reallegendscorp9155/sqoop.password  --split-by data_value --columns series_reference,period,data_value,status,units,magnitude,subject,groups,series_title_1,series_title_2,series_title_3,series_title_4,series_title_5 --table local_finance --delete-target-dir --target-dir /user/reallegendscorp9155/murthy/project/local_data --fields-terminated-by "," --hive-import --hive-overwrite --hive-table murthy_project.local_finance



sqoop import --connect jdbc:mysql://cxln2.c.thelab-240901.internal/sqoopex --username sqoopuser --password-file /user/reallegendscorp9155/sqoop.password  --split-by data_value --columns series_reference,period,data_value,status,units,magnitude,subject,groups,series_title_1,series_title_2,series_title_3,series_title_4,series_title_5 --table central_finance --delete-target-dir --target-dir /user/reallegendscorp9155/murthy/project/central_data --fields-terminated-by "," --hive-import --hive-overwrite --hive-table murthy_project.central_finance


sqoop import --connect jdbc:mysql://cxln2.c.thelab-240901.internal/sqoopex --username sqoopuser --password-file /user/reallegendscorp9155/sqoop.password  --split-by data_value --columns series_reference,period,data_value,status,units,magnitude,subject,groups,series_title_1,series_title_2,series_title_3,series_title_4,series_title_5 --table general_finance --delete-target-dir --target-dir /user/reallegendscorp9155/murthy/project/general_data --fields-terminated-by "," --hive-import --hive-overwrite --hive-table murthy_project.general_finance


spark-submit /home/reallegendscorp9155/murthy_project/py_spark.py
