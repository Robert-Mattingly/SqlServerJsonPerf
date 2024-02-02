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
        
        let appUser = config.["Sql:AppLogin:Username"]
        let appPassword = config.["Sql:AppLogin:Password"]
        let adminConnString = config.["Sql:AdminConnectionString"]
        let dbName = Constants.DatabaseName
        
        printfn "Removing existing database if exists..."
        let server = Server()
        server.ConnectionContext.ConnectionString <- adminConnString
        ServerManagement.cleanup server appUser
        
        let (db, login, user) = ServerManagement.init
                                    server
                                    appUser
                                    appPassword
                                    dbName
        
        printfn "Creating tables..."
        TableInformation.createTables db
        
        let sampleSize = 1_000_000
        printfn "Generating %i samples..." sampleSize
        let samples = DataSeed.generateSampleData sampleSize
        
        let appConnString = $"Server=%s{server.Name};Database=%s{dbName};User Id=%s{appUser};Password=%s{appPassword};TrustServerCertificate=True"
        
        printfn "Inserting samples..."
        let bulkInsertMetrics = dict[
            "RawJson", DataWriter.bulkInsertRawJson appConnString Constants.RawJsonTableName samples
            "RawJson500", DataWriter.bulkInsertRawJson500 appConnString Constants.RawJson500TableName samples
            "JsonWithIndex", DataWriter.bulkInsertJsonWithIndex appConnString Constants.JsonWithIndexTableName samples
            "JsonWithDimensionTable", DataWriter.bulkInsertJsonWithDimension appConnString Constants.JsonWithDimensionTableName samples
            "Relational", DataWriter.bulkInsertRelational appConnString Constants.PersonTableName Constants.AddressTableName Constants.PhoneNumberTableName samples
        ] 
        
        printfn "Selecting by Country..."
        let countryCodeToSelect = 987
        let selectByCountryCodeMetrics = dict[
            "RawJson", DataReader.queryRawJsonByCountryCode appConnString countryCodeToSelect
            "RawJsonNoCrossApply", DataReader.queryRawJsonByCountryCodeWithoutCrossApply appConnString countryCodeToSelect
            "RawJson500", DataReader.queryRawJson500ByCountryCode appConnString countryCodeToSelect
            "JsonWithDimensionTable", DataReader.queryJsonWithDimensionTableByCountryCode appConnString countryCodeToSelect
            "Relational", DataReader.queryRelationalByCountryCode appConnString countryCodeToSelect
        ]
        
        printfn "Selecting by Zip..."
        let zipToSelect = "96863"
        let selectByZipMetrics = dict[
            "RawJson", DataReader.queryRawJsonByZip appConnString zipToSelect
            "RawJson500", DataReader.queryRawJson500ByZip appConnString zipToSelect
            "JsonWithIndexNoForce", DataReader.queryJsonWithIndexByZip appConnString zipToSelect false
            "JsonWithIndexForce", DataReader.queryJsonWithIndexByZip appConnString zipToSelect true
            "Relational", DataReader.queryRelationalByZip appConnString zipToSelect
        ]
        
        let createWhitespaceOnConsole lines =
            for _ in 1..lines do
                printfn ""
        
        createWhitespaceOnConsole 2
        Reporting.reportInsertMetrics bulkInsertMetrics |> printfn "%s"
        
        createWhitespaceOnConsole 2
        Reporting.reportSelectMetrics selectByCountryCodeMetrics |> printfn "%s"
        
        createWhitespaceOnConsole 2
        Reporting.reportSelectMetrics selectByZipMetrics |> printfn "%s"
        
        ServerManagement.cleanup server appUser
        
        0