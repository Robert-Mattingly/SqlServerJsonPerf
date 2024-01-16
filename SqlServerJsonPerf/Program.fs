// For more information see https://aka.ms/fsharp-console-apps

namespace SqlServerJsonPerf

open Microsoft.Extensions.Configuration
open Microsoft.SqlServer.Management.Smo
open SqlServerJsonPerf.DataReader

type Program() =
    do()

module Program =
    [<EntryPoint>]
    let main argv =
        let config = ConfigurationBuilder().AddUserSecrets<Program>().Build()
        
        let typeOfSelectMetrics = typeof<SelectMetrics>
        let fields = typeOfSelectMetrics.GetFields()
        let properties = typeOfSelectMetrics.GetProperties()
        
        let adminUser = config.["Sql:AdminLogin:Username"]
        let adminPassword = config.["Sql:AdminLogin:Password"]
        let appUser = config.["Sql:AppLogin:Username"]
        let appPassword = config.["Sql:AppLogin:Password"]
        let dbName = Constants.DatabaseName
        
        printfn "Removing existing database if exists..."
        let server = ServerManagement.openServer adminUser adminPassword
        ServerManagement.cleanup server appUser
        
        let (db, login, user) = ServerManagement.init
                                    adminUser
                                    adminPassword
                                    appUser
                                    appPassword
                                    dbName
        
        printfn "Creating tables..."
        TableInformation.createTables db
        
        let sampleSize = 1_000//_000
        printfn "Generating %i samples..." sampleSize
        let samples = DataSeed.generateSampleData sampleSize
        
        let appConnString = $"Server=%s{server.Name};Database=%s{dbName};User Id=%s{appUser};Password=%s{appPassword};TrustServerCertificate=True"
        
        printfn "Inserting samples..."
        let bulkInsertMetrics = dict[
            "RawJson", DataWriter.bulkInsertRawJson appConnString Constants.RawJsonTableName samples
            "JsonWithIndex", DataWriter.bulkInsertJsonWithIndex appConnString Constants.JsonWithIndexTableName samples
            "JsonWithDimensionTable", DataWriter.bulkInsertJsonWithDimension appConnString Constants.JsonWithDimensionTableName samples
            "Relational", DataWriter.bulkInsertRelational appConnString Constants.PersonTableName Constants.AddressTableName Constants.PhoneNumberTableName samples
        ]
        
        printfn "Selecting by Country..."
        let countryCodeToSelect = 987
        let selectByCountryCodeMetrics = dict[
            "RawJson", DataReader.queryRawJsonByCountryCode appConnString countryCodeToSelect
            "JsonWithDimensionTable", DataReader.queryJsonWithDimensionTableByCountryCode appConnString countryCodeToSelect
            "Relational", DataReader.queryRelationalByCountryCode appConnString countryCodeToSelect
        ]
        
        printfn "Selecting by Zip..."
        let zipToSelect = "96863"
        let selectByZipMetrics = dict[
            "RawJson", DataReader.queryRawJsonByZip appConnString zipToSelect
            "JsonWithIndexNoForce", DataReader.queryJsonWithIndexByZip appConnString zipToSelect false
            "JsonWithIndexForce", DataReader.queryJsonWithIndexByZip appConnString zipToSelect true
            "Relational", DataReader.queryRelationalByZip appConnString zipToSelect
        ]
        
        ServerManagement.cleanup server appUser
        
        printfn "Hello from F#"
        
        0