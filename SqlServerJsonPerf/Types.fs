module SqlServerJsonPerf.Types

open System

type Address = {
    Id: Guid
    Line1:string
    Line2:string
    City:string
    State:string
    Zip:string
}

type PhoneNumber = {
    Id: Guid
    Country: string
    AreaCode: string
    Number: string
}

type Person = {
    Id: Guid
    FirstName:string
    LastName:string
    Age:int
    Addresses:Address list
    PhoneNumbers:PhoneNumber list
}
