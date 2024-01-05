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
        let dbName = Constants.DatabaseName
        
        let server = ServerManagement.openServer adminUser adminPassword
        ServerManagement.cleanup server appUser
        
        let (db, login, user) = ServerManagement.init
                                    adminUser
                                    adminPassword
                                    appUser
                                    appPassword
                                    dbName
        
        TableInformation.createTables db
        
        let samples = DataSeed.generateSampleData 1_000_00
        
        let appConnString = $"Server=%s{server.Name};Database=%s{dbName};User Id=%s{appUser};Password=%s{appPassword};TrustServerCertificate=True"
        
        let rawJsonInsertResults = DataWriter.bulkInsertRawJson appConnString Constants.RawJsonTableName samples
        
        let dimensionInsertResults = DataWriter.bulkInsertJsonWithDimension appConnString Constants.JsonWithDimensionTableName samples
        
        let relationalInsertResults = DataWriter.bulkInsertRelational appConnString Constants.PersonTableName Constants.AddressTableName Constants.PhoneNumberTableName samples
        
        let rawJsonSelectMetrics = DataReader.queryRawJson appConnString 987
        
        let jsonWithDimensionSelectMetrics = DataReader.queryJsonWithDimensionTable appConnString 987
        
        let relationalSelectMetrics = DataReader.queryRelational appConnString 987
        
        ServerManagement.cleanup server appUser
        
        printfn "Hello from F#"
        
        0