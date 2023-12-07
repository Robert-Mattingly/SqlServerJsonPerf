// For more information see https://aka.ms/fsharp-console-apps

namespace SqlServerJsonPerf

open Microsoft.Extensions.Configuration
open Microsoft.SqlServer.Management.Smo

type Program() =
    do()

module Program =
    [<EntryPoint>]
    let main argv =
        let config = ConfigurationBuilder().AddUserSecrets<Program>().Build()
        
        let adminUser = config.["Sql:AdminLogin:Username"]
        let adminPassword = config.["Sql:AdminLogin:Password"]
        let appUser = config.["Sql:AppLogin:Username"]
        let appPassword = config.["Sql:AppLogin:Password"]
        let dbName = nameof SqlServerJsonPerf
        
        let (db, login, user) = ServerManagement.init
                                    adminUser
                                    adminPassword
                                    appUser
                                    appPassword
                                    dbName
        
        ServerManagement.cleanup db login user
        
        printfn "Hello from F#"
        
        0