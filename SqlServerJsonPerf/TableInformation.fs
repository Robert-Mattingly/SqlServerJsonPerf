module SqlServerJsonPerf.TableInformation

open Microsoft.SqlServer.Management.Smo
open SqlServerJsonPerf.Operators

let private createRawJsonTable database =
    Table(database, Constants.RawJsonTableName)
    <-| ("Json", DataType.NVarCharMax, false)
    |> create |> ignore
    
let private createRawJson500Table database =
    Table(database, Constants.RawJson500TableName)
    <-| ("Json", DataType.NVarChar(500), false)
    |> create |> ignore
    
let private createJsonWithIndexTable database =
    Table(database, Constants.JsonWithIndexTableName)
    <-| ("Json", DataType.NVarCharMax, false)
    <-/ ("Zip", DataType.VarChar(5), false, true, "CAST(JSON_VALUE(Json, 'strict $.Address.Zip') AS VARCHAR(5))")
    <-% ("ZipIndex", IndexKeyType.None, "Zip", false)
    |> create |> ignore
    
let private createJsonTableWithTriggerForDimension database =
    let personTable =
        Table(database, Constants.JsonWithDimensionTableName)
        <-| ("Json", DataType.NVarCharMax, false)
        <-/ ("Id", DataType.UniqueIdentifier, false, true, "CAST(JSON_VALUE(Json, 'strict $.Id') AS UNIQUEIDENTIFIER)")
        <-% ("IdIndex", IndexKeyType.DriPrimaryKey, "Id", true)
        |> create
    
    let phoneDimensionTable =
        Table(database, Constants.PhoneDimensionTableName)
        <-| ("PersonId", DataType.UniqueIdentifier, false)
        <-| ("CountryCode", DataType.NVarChar(5), false)
        |> create
    
    let personInsertTrigger = Trigger(personTable, "PersonInsertTrigger")
    personInsertTrigger.TextMode <- false
    personInsertTrigger.TextBody <- $"
    INSERT INTO {phoneDimensionTable.Name} (PersonId, CountryCode)
    SELECT
        JSON_VALUE(Json, '$.Id') AS PersonId,
        JSON_VALUE(phone.value, '$.Country') AS CountryCode
    FROM inserted
    CROSS APPLY 
        OPENJSON(Json, '$.PhoneNumbers') AS phone
    "
    personInsertTrigger.Insert <- true
    personInsertTrigger.Create()
    
let private createRelationalTables database =
    let personTable =
        Table(database, Constants.PersonTableName)
        <-| ("Id", DataType.UniqueIdentifier, false)
        <-| ("FirstName", DataType.NVarChar(50), false)
        <-| ("LastName", DataType.NVarChar(50), false)
        <-| ("Age", DataType.Int, false)
        <-% ("PersonPrimaryKey", IndexKeyType.DriPrimaryKey, "Id", true)
        |> create
    
    Table(database, Constants.PhoneNumberTableName)
    <-| ("PersonId", DataType.UniqueIdentifier, false)
    <-| ("Id", DataType.UniqueIdentifier, false)
    <-| ("Country", DataType.VarChar(5), false)
    <-| ("AreaCode", DataType.NVarChar(30), false)
    <-| ("Number", DataType.NVarChar(10), false)
    <-%% ("CountryCodeIndex", IndexKeyType.None, seq { "Country"; "PersonId" }, false)
    <-% ("PhoneNumberPrimaryKey", IndexKeyType.DriPrimaryKey, "Id", true)
    <-@ ("PhoneNumberPersonIdForeignKey", personTable.Name, "Id", "PersonId")
    |> create |> ignore
    
    Table(database, Constants.AddressTableName)
    <-| ("PersonId", DataType.UniqueIdentifier, false)
    <-| ("Id", DataType.UniqueIdentifier, false)
    <-| ("Line1", DataType.NVarChar(150), false)
    <-| ("Line2", DataType.NVarChar(150), true)
    <-| ("City", DataType.NVarChar(50), false)
    <-| ("State", DataType.NVarChar(50), false)
    <-| ("Zip", DataType.VarChar(5), false)
    <-% ("AddressPrimaryKey", IndexKeyType.DriPrimaryKey, "Id", true)
    <-@ ("AddressPersonIdForeignKey", personTable.Name, "Id", "PersonId")
    <-%% ("ZipAndPersonIdIndex", IndexKeyType.None, seq { "Zip"; "PersonId" }, false)
    |> create |> ignore
    
let createTables database =
    createRawJsonTable database
    createRawJson500Table database
    createJsonWithIndexTable database
    createJsonTableWithTriggerForDimension database
    createRelationalTables database
   

