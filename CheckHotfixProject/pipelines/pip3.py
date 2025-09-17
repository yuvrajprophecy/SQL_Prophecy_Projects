Schedule = Schedule(cron = "0 0/3 * * * ? *", timezone = "Asia/Kolkata", enabled = False)
SensorSchedule = SensorSchedule(enabled = False)

with DAG(Schedule = Schedule, SensorSchedule = SensorSchedule):
    email_hotfix_notification = Task(
        task_id = "email_hotfix_notification", 
        component = "Email", 
        body = "test bb8", 
        subject = "Hotfix pip3", 
        includeData = True, 
        fileName = "test.csv", 
        to = ["yuvraj@prophecy.io"], 
        connection = Connection(kind = "smtp", id = "smtp"), 
        fileFormat = "csv", 
        hasTemplate = False
    )
    pip3__limited_results = Task(
        task_id = "pip3__limited_results", 
        component = "Model", 
        modelName = "pip3__limited_results"
    )
    pip3__limited_results.out_0 >> email_hotfix_notification.in0
