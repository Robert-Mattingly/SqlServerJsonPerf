module SqlServerJsonPerf.DataSeed

    open System
    open Types

    let private baseRecord = {
        Id = Guid.NewGuid()
        FirstName =  "John"
        LastName = "Doe"
        Age = 45
        Addresses = [
            { Id = Guid.NewGuid(); Line1 = "123 Wallaby Way"; Line2 = ""; City = "Sydney"; State = "NSW"; Zip = "2000" }
            { Id = Guid.NewGuid(); Line1 = "456 Wallaby Way"; Line2 = ""; City = "Sydney"; State = "NSW"; Zip = "2000" }
        ]
        PhoneNumbers = [
            { Id = Guid.NewGuid(); Country = "1"; AreaCode = "776"; Number = "788-2529" }
            { Id = Guid.NewGuid(); Country = "1"; AreaCode = "776"; Number = "788-2529" } 
        ]
    }
    
    let private generatePhoneNumbers (random:Random) =
        let count = random.Next(1, 3)
        let rec generatePhoneNumbers' (count: int) (acc: PhoneNumber list) =
            match count with
            | 0 -> acc
            | _ -> generatePhoneNumbers' (count - 1) ({ Id = Guid.NewGuid(); Country = random.Next(1,999).ToString(); AreaCode = "776"; Number = "788-2529" } :: acc)
        generatePhoneNumbers' count []
        
    let generateSampleData (count: int) =
        let random = new Random()
        let rec generateSampleData' (count: int) (acc: Person list) =
            match count with
            | 0 -> acc
            | _ -> generateSampleData' (count - 1) ({ baseRecord with Id = Guid.NewGuid(); PhoneNumbers = generatePhoneNumbers random } :: acc)
        generateSampleData' count []

