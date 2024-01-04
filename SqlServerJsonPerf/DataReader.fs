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