module SqlServerJsonPerf.DataWriter

open System
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
    bulkCopy.BatchSize <- 50_000
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
    bulkCopy.BatchSize <- 50_000
    bulkCopy.WriteToServer(dataTable)
    bulkInsertTimer.Stop()
    totalTimer.Stop()
    (totalTimer.Elapsed, serializationTimer.Elapsed, dataTableTimer.Elapsed, bulkInsertTimer.Elapsed)
    
let bulkInsertRelational
        (connectionString:string)
        (personTableName:string)
        (addressTableName:string)
        (phoneNumbersTableName:string)
        (people:Person list) =
    let totalTimer = Stopwatch.StartNew()
    let serializationTimer = Stopwatch()
    let dataTableTimer = Stopwatch.StartNew()
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
        person.Addresses |> List.iter (fun address ->
            addressTable.Rows.Add(person.Id, address.Id, address.Line1, address.Line2, address.City, address.State, address.Zip) |> ignore)
        person.PhoneNumbers |> List.iter (fun phoneNumber ->
            phoneNumbersTable.Rows.Add(person.Id, phoneNumber.Id, phoneNumber.Country, phoneNumber.AreaCode, phoneNumber.Number) |> ignore))
    dataTableTimer.Stop()
    let bulkInsertTimer = Stopwatch.StartNew()
    use connection = new SqlConnection(connectionString)
    connection.Open()
    use bulkCopy = new SqlBulkCopy(connection)
    bulkCopy.DestinationTableName <- personTableName
    bulkCopy.BatchSize <- 50_000
    bulkCopy.WriteToServer(personTable)
    bulkCopy.DestinationTableName <- addressTableName
    bulkCopy.WriteToServer(addressTable)
    bulkCopy.DestinationTableName <- phoneNumbersTableName
    bulkCopy.WriteToServer(phoneNumbersTable)
    bulkInsertTimer.Stop()
    totalTimer.Stop()
    (totalTimer.Elapsed, serializationTimer.Elapsed, dataTableTimer.Elapsed, bulkInsertTimer.Elapsed)