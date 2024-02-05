module SqlServerJsonPerf.DataSeed

    open System
    open System.Collections.Generic
    open Types

    let private baseRecord = {
        Id = Guid.NewGuid()
        FirstName =  "John"
        LastName = "Doe"
        Age = 45
        Address = { Id = Guid.NewGuid(); Line1 = "123 Wallaby Way"; Line2 = ""; City = "Sydney"; State = "NSW"; Zip = "96863" }
        PhoneNumbers = List<PhoneNumber>()
    }
    
    let private generateAddress (random:Random) =
        { Id = Guid.NewGuid(); Line1 = "123 Wallaby Way"; Line2 = ""; City = "Sydney"; State = "NSW"; Zip = "9686" + random.Next(1, 9).ToString() }
    
    let private generatePhoneNumbers (random:Random) =
        let count = random.Next(1, 3)
        let list = List<PhoneNumber>()
        for i = 1 to count do list.Add({ Id = Guid.NewGuid(); Country = random.Next(1,999).ToString(); AreaCode = "776"; Number = "788-2529" })
        list

        
    let generateSampleData (count: int) =
        let random = new Random()
        let rec generateSampleData' (count: int) (acc: Person list) =
            match count with
            | 0 -> acc
            | _ -> generateSampleData' (count - 1) ({ baseRecord with Id = Guid.NewGuid(); PhoneNumbers = generatePhoneNumbers random; Address = generateAddress random } :: acc)
        generateSampleData' count []

