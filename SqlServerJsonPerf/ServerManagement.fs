module SqlServerJsonPerf.ServerManagement

    open Microsoft.SqlServer.Management.Smo

    let private openServer username password =
        let server = new Server()
        server.ConnectionContext.LoginSecure <- false
        server.ConnectionContext.Login <- username
        server.ConnectionContext.Password <- password
        server
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
        // server.Refresh()
        login, user
        
    let init adminUser adminPassword appUser appPassword dbName =
        let server = openServer adminUser adminPassword
        let database = createDatabase server dbName
        let login, user = createLoginAndUser server database appUser appPassword
        database, login, user
        
    let cleanup (database:Database) (login:Login) (user:User) =
        user.DropIfExists()
        login.DropIfExists()
        database.DropIfExists()
