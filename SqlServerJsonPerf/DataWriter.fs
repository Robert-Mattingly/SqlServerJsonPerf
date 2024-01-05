module SqlServerJsonPerf.DataWriter

open System
open System.Collections
open System.Data
open System.Diagnostics
open System.Text.Json
open Microsoft.Data.SqlClient
open SqlServerJsonPerf.Extensions
open SqlServerJsonPerf.Types

type BulkInsertMetrics = {
    TotalTime:TimeSpan
    SerializationTime:TimeSpan
    BytesReceived:int64
    BytesSent:int64
    ExecutionTime:TimeSpan
    NetworkServerTime:TimeSpan
    IduCount:int64
    IduRows:int64
    ServerRoundTrips:int64
}

let private bulkCopyBatchSize = 100_000

let private parseMetrics totalTime serializationTime (statistics:IDictionary) =
    {
        TotalTime = totalTime
        SerializationTime = serializationTime
        BytesReceived = statistics.["BytesReceived"] :?> int64
        BytesSent = statistics.["BytesSent"] :?> int64
        ExecutionTime = statistics.["ExecutionTime"] :?> int64 |> float |> TimeSpan.FromMilliseconds
        NetworkServerTime = statistics.["NetworkServerTime"] :?> int64 |> float |> TimeSpan.FromMilliseconds
        IduCount = statistics.["IduCount"] :?> int64
        IduRows = statistics.["IduRows"] :?> int64
        ServerRoundTrips = statistics.["ServerRoundtrips"] :?> int64 
    }
    

   
let private prepConnection (connectionString:string)  =
    let connection = new SqlConnection(connectionString)
    connection.StatisticsEnabled <- true
    connection.Open()
    connection
    
let private prepBulkCopy (connection:SqlConnection) copyOptions =
    let bulkCopy = new SqlBulkCopy(connection, copyOptions, null)
    bulkCopy.BulkCopyTimeout <- 90
    bulkCopy.BatchSize <- bulkCopyBatchSize
    bulkCopy.NotifyAfter <- bulkCopyBatchSize / 4
    bulkCopy.SqlRowsCopied.Add(fun args -> printf ".")
    bulkCopy

let bulkInsertRawJson (connectionString:string) (tableName:string) (people:Person list) =
    let totalTimer = Stopwatch.StartNew()
    let serializationTimer = Stopwatch.StartNew()
    let jsonValues = people |> List.map JsonSerializer.Serialize
    serializationTimer.Stop()
    use dataTable = new DataTable()
    dataTable.Columns.Add("Json", typeof<string>) |> ignore
    jsonValues |> List.iter (fun json -> dataTable.Rows.Add(json) |> ignore)
    serializationTimer.Stop()
    use connection = prepConnection connectionString
    use bulkCopy = prepBulkCopy connection SqlBulkCopyOptions.Default
    bulkCopy.WriteTo tableName dataTable
    totalTimer.Stop()
    connection.RetrieveStatistics() |> parseMetrics totalTimer.Elapsed serializationTimer.Elapsed 

let bulkInsertJsonWithIndex (connectionString:string) (tableName:string) (people:Person list) =
    let totalTimer = Stopwatch.StartNew()
    let serializationTimer = Stopwatch.StartNew()
    let jsonValues = people |> List.map JsonSerializer.Serialize
    use dataTable = new DataTable()
    dataTable.Columns.Add("Json", typeof<string>) |> ignore
    jsonValues |> List.iter (fun json -> dataTable.Rows.Add(json) |> ignore)
    serializationTimer.Stop()
    use connection = prepConnection connectionString
    use bulkCopy = prepBulkCopy connection SqlBulkCopyOptions.Default
    bulkCopy.WriteTo tableName dataTable
    totalTimer.Stop()
    connection.RetrieveStatistics() |> parseMetrics totalTimer.Elapsed serializationTimer.Elapsed

let bulkInsertJsonWithDimension (connectionString:string) (tableName:string) (people:Person list) =
    let totalTimer = Stopwatch.StartNew()
    let serializationTimer = Stopwatch.StartNew()
    let jsonValues = people |> List.map JsonSerializer.Serialize
    use dataTable = new DataTable()
    dataTable.Columns.Add("Json", typeof<string>) |> ignore
    jsonValues |> List.iter (fun json -> dataTable.Rows.Add(json) |> ignore)
    serializationTimer.Stop()
    use connection = prepConnection connectionString
    // Note: SqlBulkCopyOptions.FireTriggers is required for the dimension to work
    // and the SqlBulkCopy constructor that takes a SqlBulkCopyOptions parameter
    // and a SqlConnection requires a SqlTransaction parameter.
    use bulkCopy = prepBulkCopy connection SqlBulkCopyOptions.FireTriggers
    bulkCopy.WriteTo tableName dataTable
    totalTimer.Stop()
    connection.RetrieveStatistics() |> parseMetrics totalTimer.Elapsed serializationTimer.Elapsed
    
let bulkInsertRelational
        (connectionString:string)
        (personTableName:string)
        (addressTableName:string)
        (phoneNumbersTableName:string)
        (people:Person list) =
    let totalTimer = Stopwatch.StartNew()
    let serializationTimer = Stopwatch()
    use personTable = new DataTable()
    personTable.Columns.Add("Id", typeof<Guid>) |> ignore
    personTable.Columns.Add("FirstName", typeof<string>) |> ignore
    personTable.Columns.Add("LastName", typeof<string>) |> ignore
    personTable.Columns.Add("Age", typeof<int>) |> ignore
    use addressTable = new DataTable()
    addressTable.Columns.Add("PersonId", typeof<Guid>) |> ignore
    addressTable.Columns.Add("Id", typeof<Guid>) |> ignore
    addressTable.Columns.Add("Line1", typeof<string>) |> ignore
    addressTable.Columns.Add("Line2", typeof<string>) |> ignore
    addressTable.Columns.Add("City", typeof<string>) |> ignore
    addressTable.Columns.Add("State", typeof<string>) |> ignore
    addressTable.Columns.Add("Zip", typeof<string>) |> ignore
    use phoneNumbersTable = new DataTable()
    phoneNumbersTable.Columns.Add("PersonId", typeof<Guid>) |> ignore
    phoneNumbersTable.Columns.Add("Id", typeof<Guid>) |> ignore
    phoneNumbersTable.Columns.Add("Country", typeof<string>) |> ignore
    phoneNumbersTable.Columns.Add("AreaCode", typeof<string>) |> ignore
    phoneNumbersTable.Columns.Add("Number", typeof<string>) |> ignore
    people |> List.iter (fun person ->
        personTable.Rows.Add(person.Id, person.FirstName, person.LastName, person.Age) |> ignore
        let address = person.Address
        addressTable.Rows.Add(person.Id, address.Id, address.Line1, address.Line2, address.City, address.State, address.Zip) |> ignore
        person.PhoneNumbers |> List.iter (fun phoneNumber ->
            phoneNumbersTable.Rows.Add(person.Id, phoneNumber.Id, phoneNumber.Country, phoneNumber.AreaCode, phoneNumber.Number) |> ignore))
    serializationTimer.Stop()
    use connection = prepConnection connectionString
    use bulkCopy = prepBulkCopy connection SqlBulkCopyOptions.Default
    bulkCopy.WriteTo personTableName personTable
    bulkCopy.WriteTo addressTableName addressTable
    bulkCopy.WriteTo phoneNumbersTableName phoneNumbersTable
    totalTimer.Stop()
    connection.RetrieveStatistics() |> parseMetrics totalTimer.Elapsed serializationTimer.Elapsed