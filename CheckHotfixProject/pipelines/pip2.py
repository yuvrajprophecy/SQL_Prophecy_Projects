Config = {"config1" : "'Pipeline 2 Config 1 Original'", "config2" : "'Pipeline 2 Config 2 Original'"}
Schedule = Schedule(cron = "0 0/3 * * * ? *", timezone = "Asia/Kolkata")
SensorSchedule = SensorSchedule(enabled = False)

with DAG(Config = Config, Schedule = Schedule, SensorSchedule = SensorSchedule):
    email_hotfix_notification = Task(
        task_id = "email_hotfix_notification", 
        component = "Email", 
        body = "", 
        subject = "Hotfix pip2", 
        includeData = True, 
        fileName = "test.csv", 
        to = ["yuvraj@prophecy.io"], 
        connection = Connection(kind = "smtp", id = "smtp"), 
        fileFormat = "csv", 
        hasTemplate = False
    )
    pip2__limited_results = Task(
        task_id = "pip2__limited_results", 
        component = "Model", 
        modelName = "pip2__limited_results"
    )
    pip2__limited_results.out_0 >> email_hotfix_notification.in0
