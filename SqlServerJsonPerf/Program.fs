// For more information see https://aka.ms/fsharp-console-apps

namespace SqlServerJsonPerf

open Microsoft.Extensions.Configuration

type Program() =
    do()

module Program =
    [<EntryPoint>]
    let main argv =
        let config = ConfigurationBuilder().AddUserSecrets<Program>().Build()
        
        let username = config.["Sql:Username"]
        let password = config.["Sql:Password"]
        
        printfn $"Running as {username}"
        
        printfn "Hello from F#"
        
        0