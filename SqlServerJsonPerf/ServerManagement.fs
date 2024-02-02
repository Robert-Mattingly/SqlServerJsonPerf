module SqlServerJsonPerf.ServerManagement

    open Microsoft.SqlServer.Management.Smo

    let private createDatabase server dbName =
        let database = new Database(server, dbName)
        database.Create()
        database
    
    let private createLoginAndUser server database username (password:string) =
        
        let login = new Login(server, username)
        login.LoginType <- LoginType.SqlLogin
        login.Create(password)
        
        let user = new User(database, username)
        user.Login <- username
        user.Create()
        user.AddToRole("db_datareader")
        user.AddToRole("db_datawriter")
        user.Alter()
        login, user
        
    let init server appUser appPassword dbName =
        let database = createDatabase server dbName
        let login, user = createLoginAndUser server database appUser appPassword
        database, login, user
        
    let cleanup (server:Server) (username:string) =
        let database = server.Databases.[Constants.DatabaseName]
        if database <> null then
            server.ConnectionContext.ExecuteNonQuery($"ALTER DATABASE [{Constants.DatabaseName}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE") |> ignore
            database.Drop()
        let login = server.Logins.[username]
        if login <> null then
            login.Drop()
