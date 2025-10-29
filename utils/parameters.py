# Create parameters to dynamically run the autoloader pipeline
source_datasets = [
    {"file_name":"products"},
    {"file_name":"sales"},
    {"file_name":"stores"},
    {"file_name":"warranty"},
    {"file_name":"category"}
]

dbutils.jobs.taskValues.set("source_datasets", source_datasets)
