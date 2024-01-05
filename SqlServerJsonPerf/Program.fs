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
        
        let bulkInsertMetrics = [
            "RawJson", DataWriter.bulkInsertRawJson appConnString Constants.RawJsonTableName samples
            "JsonWithIndex", DataWriter.bulkInsertJsonWithIndex appConnString Constants.JsonWithIndexTableName samples
            "JsonWithDimensionTable", DataWriter.bulkInsertJsonWithDimension appConnString Constants.JsonWithDimensionTableName samples
            "Relational", DataWriter.bulkInsertRelational appConnString Constants.PersonTableName Constants.AddressTableName Constants.PhoneNumberTableName samples
        ]
        
        let countryCodeToSelect = 987
        let selectByCountryCodeMetrics = [
            "RawJson", DataReader.queryRawJsonByCountryCode appConnString countryCodeToSelect
            "JsonWithDimensionTable", DataReader.queryJsonWithDimensionTableByCountryCode appConnString countryCodeToSelect
            "Relational", DataReader.queryRelationalByCountryCode appConnString countryCodeToSelect
        ]
        
        let zipToSelect = "96863"
        let selectByZipMetrics = [
            "RawJson", DataReader.queryRawJsonByZip appConnString zipToSelect
            "JsonWithIndexNoForce", DataReader.queryJsonWithIndexByZip appConnString zipToSelect false
            "JsonWithIndexForce", DataReader.queryJsonWithIndexByZip appConnString zipToSelect true
            "Relational", DataReader.queryRelationalByZip appConnString zipToSelect
        ]
        
        ServerManagement.cleanup server appUser
        
        printfn "Hello from F#"
        
        0