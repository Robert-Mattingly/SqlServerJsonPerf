module SqlServerJsonPerf.DataWriter

open System.Data
open System.Diagnostics
open System.Text.Json
open Microsoft.Data.SqlClient
open SqlServerJsonPerf.Types

let bulkInsertRawJson (connectionString:string) (tableName:string) (people:Person list) =
    let totalTimer = Stopwatch.StartNew()
    let serializationTimer = Stopwatch.StartNew()
    let jsonValues = people |> List.map JsonSerializer.Serialize
    serializationTimer.Stop()
    let dataTableTimer = Stopwatch.StartNew()
    use dataTable = new DataTable()
    dataTable.Columns.Add("Json", typeof<string>) |> ignore
    jsonValues |> List.iter (fun json -> dataTable.Rows.Add(json) |> ignore)
    dataTableTimer.Stop()
    let bulkInsertTimer = Stopwatch.StartNew()
    use connection = new SqlConnection(connectionString)
    connection.Open()
    use bulkCopy = new SqlBulkCopy(connection)
    bulkCopy.DestinationTableName <- tableName
    bulkCopy.BatchSize <- people.Length / 5
    bulkCopy.WriteToServer(dataTable)
    bulkInsertTimer.Stop()
    totalTimer.Stop()
    (totalTimer.Elapsed, serializationTimer.Elapsed, dataTableTimer.Elapsed, bulkInsertTimer.Elapsed)

let bulkInsertJsonWithDimension (connectionString:string) (tableName:string) (people:Person list) =
    let totalTimer = Stopwatch.StartNew()
    let serializationTimer = Stopwatch.StartNew()
    let jsonValues = people |> List.map JsonSerializer.Serialize
    serializationTimer.Stop()
    let dataTableTimer = Stopwatch.StartNew()
    use dataTable = new DataTable()
    dataTable.Columns.Add("Json", typeof<string>) |> ignore
    jsonValues |> List.iter (fun json -> dataTable.Rows.Add(json) |> ignore)
    dataTableTimer.Stop()
    let bulkInsertTimer = Stopwatch.StartNew()
    use connection = new SqlConnection(connectionString)
    connection.Open()
    // Note: SqlBulkCopyOptions.FireTriggers is required for the dimension to work
    // and the SqlBulkCopy constructor that takes a SqlBulkCopyOptions parameter
    // and a SqlConnection requires a SqlTransaction parameter.
    use bulkCopy = new SqlBulkCopy(connection, SqlBulkCopyOptions.FireTriggers, null)
    bulkCopy.DestinationTableName <- tableName
    bulkCopy.BatchSize <- people.Length / 5
    bulkCopy.WriteToServer(dataTable)
    bulkInsertTimer.Stop()
    totalTimer.Stop()
    (totalTimer.Elapsed, serializationTimer.Elapsed, dataTableTimer.Elapsed, bulkInsertTimer.Elapsed)