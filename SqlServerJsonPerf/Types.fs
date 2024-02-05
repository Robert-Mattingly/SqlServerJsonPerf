module SqlServerJsonPerf.Types

open System
open System.Collections.Generic

[<CLIMutable>]
type Address = {
    Id: Guid
    Line1:string
    Line2:string
    City:string
    State:string
    Zip:string
}

[<CLIMutable>]
type PhoneNumber = {
    Id: Guid
    Country: string
    AreaCode: string
    Number: string
}

[<CLIMutable>]
type Person = {
    Id: Guid
    FirstName:string
    LastName:string
    Age:int
    Address:Address
    PhoneNumbers:List<PhoneNumber>
}
