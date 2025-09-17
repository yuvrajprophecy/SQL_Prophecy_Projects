Schedule = Schedule(cron = "0 0/5 * * * ? *", timezone = "Asia/Kolkata", enabled = False)
SensorSchedule = SensorSchedule(enabled = False)

with DAG(Schedule = Schedule, SensorSchedule = SensorSchedule):
    pip3__limited_results = Task(
        task_id = "pip3__limited_results", 
        component = "Model", 
        modelName = "pip3__limited_results"
    )
    email_hotfix_notification = Task(
        task_id = "email_hotfix_notification", 
        component = "Email", 
        body = "", 
        subject = "Hotfix pip3", 
        includeData = True, 
        fileName = "test.csv", 
        to = ["yuvraj@prophecy.io"], 
        connection = Connection(kind = "smtp", id = "smtp"), 
        fileFormat = "csv", 
        hasTemplate = False
    )
    pip3__limited_results.out_0 >> email_hotfix_notification.in0
