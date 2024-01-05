module SqlServerJsonPerf.DataReader

open System
open System.Collections
open System.Data
open System.Diagnostics
open System.Text.Json
open Microsoft.Data.SqlClient
open SqlServerJsonPerf.Types

type SelectMetrics = {
    TotalTime:TimeSpan
    DeserializationTime:TimeSpan
    BytesReceived:int64
    BytesSent:int64
    ExecutionTime:TimeSpan
    NetworkServerTime:TimeSpan
    SelectCount:int64
    SelectRows:int64
    ServerRoundTrips:int64
    SumResultSets:int64
    Results: Person list
}

let private parseMetrics totalTime deserializationTime results (statistics:IDictionary) =
    {
        TotalTime = totalTime
        DeserializationTime = deserializationTime
        BytesReceived = statistics.["BytesReceived"] :?> int64
        BytesSent = statistics.["BytesSent"] :?> int64
        ExecutionTime = statistics.["ExecutionTime"] :?> int64 |> float |> TimeSpan.FromMilliseconds
        NetworkServerTime = statistics.["NetworkServerTime"] :?> int64 |> float |> TimeSpan.FromMilliseconds
        SelectCount = statistics.["SelectCount"] :?> int64
        SelectRows = statistics.["SelectRows"] :?> int64
        ServerRoundTrips = statistics.["ServerRoundtrips"] :?> int64
        SumResultSets = statistics.["SumResultSets"] :?> int64
        Results = results 
    }
let queryRawJson (connString:string) (countryCode:int) =
    let totalTimer = Stopwatch.StartNew()
    let sql = $"
    SELECT
        [Json]
    FROM {Constants.RawJsonTableName}
    CROSS APPLY OPENJSON(Json, '$.PhoneNumbers') AS phone
    WHERE JSON_VALUE(phone.value, '$.Country') = @TargetCode
    "
    use conn = new SqlConnection(connString)
    conn.StatisticsEnabled <- true
    conn.Open()
    use cmd = new SqlCommand(sql, conn)
    cmd.Parameters.Add("@TargetCode", SqlDbType.VarChar, 5).Value <- countryCode.ToString()
    use reader = cmd.ExecuteReader()
    let rec readAll (reader:SqlDataReader) =
        seq {
            if reader.Read() then
                yield reader.GetString(0)
                yield! readAll reader
        }
    let rawJson = readAll reader |> Seq.toList
    let deserializationTimer = Stopwatch.StartNew()
    let deserialized = rawJson |> List.map JsonSerializer.Deserialize<Person>
    deserializationTimer.Stop()
    totalTimer.Stop()
    let metrics = conn.RetrieveStatistics()
                     |> parseMetrics totalTimer.Elapsed deserializationTimer.Elapsed deserialized
    metrics
    
let queryJsonWithDimensionTable (connString:string) (countryCode:int) =
    let totalTimer = Stopwatch.StartNew()
    let sql = $"
    SELECT
        jwd.Json
    FROM {Constants.JsonWithDimensionTableName} jwd
    JOIN {Constants.PhoneDimensionTableName} pd ON jwd.Id = pd.PersonId
    WHERE pd.CountryCode = @TargetCode
    "
    use conn = new SqlConnection(connString)
    conn.StatisticsEnabled <- true
    conn.Open()
    use cmd = new SqlCommand(sql, conn)
    cmd.Parameters.Add("@TargetCode", SqlDbType.VarChar, 5).Value <- countryCode.ToString()
    use reader = cmd.ExecuteReader()
    let rec readAll (reader:SqlDataReader) =
        seq {
            if reader.Read() then
                yield reader.GetString(0)
                yield! readAll reader
        }
    let rawJson = readAll reader |> Seq.toList
    let deserializationTimer = Stopwatch.StartNew()
    let deserialized = rawJson |> List.map JsonSerializer.Deserialize<Person>
    deserializationTimer.Stop()
    totalTimer.Stop()
    let metrics = conn.RetrieveStatistics()
                     |> parseMetrics totalTimer.Elapsed deserializationTimer.Elapsed deserialized
    metrics

let convertToMap (seq: ('a * 'b) seq) : Map<'a, 'b list> =
    seq
    |> Seq.groupBy fst
    |> Seq.map (fun (key, group) -> key, (group |> Seq.map snd |> List.ofSeq))
    |> Map.ofSeq

let private parseAddressTableToMap (addresses:DataTable) =
    let addressesByPersonId = addresses.Rows |> Seq.cast<DataRow> |> Seq.map (fun row ->
        row.["PersonId"] :?> Guid,
        {
            Id = row.["Id"] :?> Guid
            Line1 = row.["Line1"] :?> string
            Line2 = row.["Line2"] :?> string
            City = row.["City"] :?> string
            State = row.["State"] :?> string
            Zip = row.["Zip"] :?> string
        })
    addressesByPersonId |> convertToMap
    
let private parsePhoneNumberTableToMap (phoneNumbers:DataTable) =
    let phoneNumbersByPersonId = phoneNumbers.Rows |> Seq.cast<DataRow> |> Seq.map (fun row ->
        row.["PersonId"] :?> Guid,
        {
            Id = row.["Id"] :?> Guid
            Country = row.["Country"] :?> string
            AreaCode = row.["AreaCode"] :?> string
            Number = row.["Number"] :?> string
        })
    phoneNumbersByPersonId |> convertToMap

let private deserializeDataTablesToPersonList (persons:DataTable) (addresses:DataTable) (phoneNumbers:DataTable) =
    let addressesByPersonId = parseAddressTableToMap addresses
    let phoneNumbersByPersonId = parsePhoneNumberTableToMap phoneNumbers
    let results = persons.Rows |> Seq.cast<DataRow> |> Seq.map (fun row ->
        let id = row.["Id"] :?> Guid
        {
            Id = id
            FirstName = row.["FirstName"] :?> string
            LastName = row.["LastName"] :?> string
            Age = row.["Age"] :?> int
            Addresses = addressesByPersonId.[id]
            PhoneNumbers = phoneNumbersByPersonId.[id]
        })
    results |> List.ofSeq
    
let queryRelational (connString:string) (countryCode:int) =
    let totalTimer = Stopwatch.StartNew()
    let sql = $"
    SELECT p.* FROM {Constants.PersonTableName} p
    JOIN {Constants.PhoneNumberTableName} pn ON p.Id = pn.PersonId
    WHERE pn.Country = @TargetCode

    SELECT a.* FROM {Constants.AddressTableName} a
    JOIN {Constants.PhoneNumberTableName} pn ON a.PersonId = pn.PersonId
    WHERE pn.Country = @TargetCode

    SELECT * FROM {Constants.PhoneNumberTableName}
    WHERE Country = @TargetCode
    "
    use conn = new SqlConnection(connString)
    conn.StatisticsEnabled <- true
    conn.Open()
    use dataset = new DataSet()
    use adapter = new SqlDataAdapter(sql, conn)
    adapter.SelectCommand.Parameters.Add("@TargetCode", SqlDbType.VarChar, 5).Value <- countryCode.ToString()
    adapter.Fill(dataset) |> ignore
    let deserializationTimer = Stopwatch.StartNew()
    let deserialized = deserializeDataTablesToPersonList dataset.Tables.[0] dataset.Tables.[1] dataset.Tables.[2]
    deserializationTimer.Stop()
    totalTimer.Stop()
    let metrics = conn.RetrieveStatistics()
                     |> parseMetrics totalTimer.Elapsed deserializationTimer.Elapsed deserialized
    metrics