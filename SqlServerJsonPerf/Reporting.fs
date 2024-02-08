module SqlServerJsonPerf.Reporting

open System
open System.Collections.Generic
open SqlServerJsonPerf.DataReader
open SqlServerJsonPerf.DataWriter

let columnPad = 22

let prepTimeAndPercentString columnPad totalMs percent =
    let percentPadded = (sprintf "%0.2f%%" percent).PadLeft(7)
    let raw = sprintf "%0.2fms / %s" totalMs percentPadded
    raw.PadLeft(columnPad)

let reportInsertMetrics (results:IDictionary<string,BulkInsertMetrics>) =
    let maxTotalTime = results.Values |> Seq.map _.TotalTime.Ticks |> Seq.max
    let maxSerializationTime = results.Values |> Seq.map _.SerializationTime.Ticks |> Seq.max
    let maxConnectionTime = results.Values |> Seq.map _.ConnectionTime.Ticks |> Seq.max
    let maxExecutionTime = results.Values |> Seq.map _.ExecutionTime.Ticks |> Seq.max
    
    let maxLabelWidth = results.Keys |> Seq.map _.Length |> Seq.max
    
    let header = seq {
        sprintf "%s | %s | %s | %s | %s | %s |"
                ("BulkInsert".PadLeft(maxLabelWidth))
                ("Total".PadLeft(columnPad))
                ("Serialization".PadLeft(columnPad))
                ("ConnectionTime".PadLeft(columnPad))
                ("ExecutionTime".PadLeft(columnPad))
                ("BytesSent (KB)".PadLeft(columnPad))
    }
    let lines = seq {
        for entry in results do
            let label = entry.Key.PadLeft(maxLabelWidth)
            let metrics = entry.Value
            let totalTimePercent = (float metrics.TotalTime.Ticks / float maxTotalTime) * 100.0
            let serializationTimePercent = (float metrics.SerializationTime.Ticks / float maxSerializationTime) * 100.0
            let connectionTimePercent = (float metrics.ConnectionTime.Ticks / float maxConnectionTime) * 100.0
            let executionTimePercent = (float metrics.ExecutionTime.Ticks / float maxExecutionTime) * 100.0
            sprintf "%s | %s | %s | %s | %s | %s |"
                    label
                    (prepTimeAndPercentString columnPad metrics.TotalTime.TotalMilliseconds totalTimePercent)
                    (prepTimeAndPercentString columnPad metrics.SerializationTime.TotalMilliseconds serializationTimePercent)
                    (prepTimeAndPercentString columnPad metrics.ConnectionTime.TotalMilliseconds connectionTimePercent)
                    (prepTimeAndPercentString columnPad metrics.ExecutionTime.TotalMilliseconds executionTimePercent)
                    ((sprintf "%d" (metrics.BytesSent / 1024L)).PadLeft(columnPad))
    }
    lines |> Seq.append header |> String.concat Environment.NewLine
    
let reportSelectMetrics<'a> (results:IDictionary<string,SelectMetrics<'a>>) =
    let maxTotalTime = results.Values |> Seq.map _.TotalTime.Ticks |> Seq.max
    let maxDeserializationTime = results.Values |> Seq.map _.DeserializationTime.Ticks |> Seq.max
    let maxConnectionTime = results.Values |> Seq.map _.ConnectionTime.Ticks |> Seq.max
    let maxExecutionTime = results.Values |> Seq.map _.ExecutionTime.Ticks |> Seq.max
    
    let maxLabelWidth = results.Keys |> Seq.map _.Length |> Seq.max
    
    let header = seq {
        sprintf "%s | %s | %s | %s | %s | %s | %s |"
                ("Select".PadLeft(maxLabelWidth))
                ("Total".PadLeft(columnPad))
                ("Deserialization".PadLeft(columnPad))
                ("ConnectionTime".PadLeft(columnPad))
                ("ExecutionTime".PadLeft(columnPad))
                ("BytesReceived (KB)".PadLeft(columnPad))
                ("SelectRows".PadLeft(columnPad))
    }
    let lines = seq {
        for entry in results do
            let label = entry.Key.PadLeft(maxLabelWidth)
            let metrics = entry.Value
            let totalTimePercent = (float metrics.TotalTime.Ticks / float maxTotalTime) * 100.0
            let deserializationTimePercent = (float metrics.DeserializationTime.Ticks / float maxDeserializationTime) * 100.0
            let connectionTimePercent = (float metrics.ConnectionTime.Ticks / float maxConnectionTime) * 100.0
            let executionTimePercent = (float metrics.ExecutionTime.Ticks / float maxExecutionTime) * 100.0
            sprintf "%s | %s | %s | %s | %s | %s | %s |"
                    label
                    (prepTimeAndPercentString columnPad metrics.TotalTime.TotalMilliseconds totalTimePercent)
                    (prepTimeAndPercentString columnPad metrics.DeserializationTime.TotalMilliseconds deserializationTimePercent)
                    (prepTimeAndPercentString columnPad metrics.ConnectionTime.TotalMilliseconds connectionTimePercent)
                    (prepTimeAndPercentString columnPad metrics.ExecutionTime.TotalMilliseconds executionTimePercent)
                    ((sprintf "%d" (metrics.BytesReceived / 1024L)).PadLeft(columnPad))
                    ((sprintf "%d" metrics.SelectRows).PadLeft(columnPad))
    }
    lines |> Seq.append header |> String.concat Environment.NewLine