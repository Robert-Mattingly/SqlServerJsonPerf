module SqlServerJsonPerf.Extensions

 open System.Data
 open System.Diagnostics

 type Microsoft.Data.SqlClient.SqlBulkCopy with
    member this.WriteTo targetTable (dataTable:DataTable) =
        printf "Writing %d rows to %s" dataTable.Rows.Count targetTable
        this.DestinationTableName <- targetTable
        this.WriteToServer(dataTable)
        printfn "Complete"