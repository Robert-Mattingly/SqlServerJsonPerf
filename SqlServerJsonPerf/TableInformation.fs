module SqlServerJsonPerf.TableInformation

open Microsoft.SqlServer.Management.Smo

let private createRawJsonTable database =
    let table = Table(database, Constants.RawJsonTableName)
    let jsonColumn = Column(table, "Json", DataType.NVarCharMax, Nullable = false)
    table.Columns.Add(jsonColumn)
    table.Create()
    
let private createJsonTableWithIndexedViews database =
    let personTable = Table(database, Constants.JsonWithDimensionTableName)
    let jsonColumn = Column(personTable, "Json", DataType.NVarCharMax, Nullable = false)
    personTable.Columns.Add(jsonColumn)
    let idColumn = Column(personTable, "Id", DataType.UniqueIdentifier, Nullable = false)
    idColumn.Computed <- true
    idColumn.IsPersisted <- true
    idColumn.ComputedText <- "CAST(JSON_VALUE(Json, 'strict $.Id') AS UNIQUEIDENTIFIER)"
    personTable.Columns.Add(idColumn)
    personTable.Create()
    let idIndex = Index(personTable, "IdIndex")
    idIndex.IndexKeyType <- IndexKeyType.DriPrimaryKey
    idIndex.IndexedColumns.Add(IndexedColumn(idIndex, "Id"))
    personTable.Indexes.Add(idIndex)
    idIndex.Create()
    
    let phoneDimensionTable = Table(database, Constants.PhoneDimensionTableName)
    let personIdColumn = Column(phoneDimensionTable, "PersonId", DataType.UniqueIdentifier, Nullable = false)
    phoneDimensionTable.Columns.Add(personIdColumn)
    let countryCodeColumn = Column(phoneDimensionTable, "CountryCode", DataType.NVarChar(5), Nullable = false)
    phoneDimensionTable.Columns.Add(countryCodeColumn)
    phoneDimensionTable.Create()
    
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
    let personTable = Table(database, Constants.PersonTableName)
    let idColumn = Column(personTable, "Id", DataType.UniqueIdentifier, Nullable = false)
    personTable.Columns.Add(idColumn)
    let firstNameColumn = Column(personTable, "FirstName", DataType.NVarChar(50), Nullable = false)
    personTable.Columns.Add(firstNameColumn)
    let lastNameColumn = Column(personTable, "LastName", DataType.NVarChar(50), Nullable = false)
    personTable.Columns.Add(lastNameColumn)
    let ageColumn = Column(personTable, "Age", DataType.Int, Nullable = false)
    personTable.Columns.Add(ageColumn)
    let personPrimaryKey = Index(personTable, "PersonPrimaryKey")
    // Primary key
    personPrimaryKey.IndexKeyType <- IndexKeyType.DriPrimaryKey
    personPrimaryKey.IndexedColumns.Add(IndexedColumn(personPrimaryKey, "Id"))
    personPrimaryKey.IsUnique <- true
    personTable.Indexes.Add(personPrimaryKey)
    personTable.Create()
    
    let phoneNumberTable = Table(database, Constants.PhoneNumberTableName)
    let personIdColumn = Column(phoneNumberTable, "PersonId", DataType.UniqueIdentifier, Nullable = false)
    phoneNumberTable.Columns.Add(personIdColumn)
    let idColumn = Column(phoneNumberTable, "Id", DataType.UniqueIdentifier, Nullable = false)
    phoneNumberTable.Columns.Add(idColumn)
    let countryColumn = Column(phoneNumberTable, "Country", DataType.NVarChar(5), Nullable = false)
    phoneNumberTable.Columns.Add(countryColumn)
    let areaCodeColumn = Column(phoneNumberTable, "AreaCode", DataType.NVarChar(30), Nullable = false)
    phoneNumberTable.Columns.Add(areaCodeColumn)
    let numberColumn = Column(phoneNumberTable, "Number", DataType.NVarChar(10), Nullable = false)
    phoneNumberTable.Columns.Add(numberColumn)
    // Primary key
    let phoneNumberPrimaryKey = Index(phoneNumberTable, "PhoneNumberPrimaryKey")
    phoneNumberPrimaryKey.IndexKeyType <- IndexKeyType.DriPrimaryKey
    phoneNumberPrimaryKey.IndexedColumns.Add(IndexedColumn(phoneNumberPrimaryKey, "Id"))
    phoneNumberPrimaryKey.IsUnique <- true
    phoneNumberTable.Indexes.Add(phoneNumberPrimaryKey)
    // Foreign key
    let phoneNumberPersonId = ForeignKey(phoneNumberTable, "PhoneNumberPersonIdForeignKey")
    phoneNumberPersonId.ReferencedTable <- personTable.Name
    let phoneNumberPersonIdColumn = ForeignKeyColumn(phoneNumberPersonId, "PersonId", "Id")
    phoneNumberPersonId.Columns.Add(phoneNumberPersonIdColumn)
    phoneNumberTable.ForeignKeys.Add(phoneNumberPersonId)
    phoneNumberTable.Create()
    
    let countryCodeIndex = Index(phoneNumberTable, "Table_CountryCodeIndex")
    countryCodeIndex.IndexedColumns.Add(IndexedColumn(countryCodeIndex, "Country"))
    countryCodeIndex.IndexedColumns.Add(IndexedColumn(countryCodeIndex, "PersonId"))
    countryCodeIndex.Create()
    
    let addressTable = Table(database, Constants.AddressTableName)
    let personIdColumn = Column(addressTable, "PersonId", DataType.UniqueIdentifier, Nullable = false)
    addressTable.Columns.Add(personIdColumn)
    let idColumn = Column(addressTable, "Id", DataType.UniqueIdentifier, Nullable = false)
    addressTable.Columns.Add(idColumn)
    let lineOneColumn = Column(addressTable, "Line1", DataType.NVarChar(150), Nullable = false)
    addressTable.Columns.Add(lineOneColumn)
    let lineTwoColumn = Column(addressTable, "Line2", DataType.NVarChar(150), Nullable = true)
    addressTable.Columns.Add(lineTwoColumn)
    let cityColumn = Column(addressTable, "City", DataType.NVarChar(50), Nullable = false)
    addressTable.Columns.Add(cityColumn)
    let stateColumn = Column(addressTable, "State", DataType.NVarChar(50), Nullable = false)
    addressTable.Columns.Add(stateColumn)
    let zipColumn = Column(addressTable, "Zip", DataType.NVarChar(50), Nullable = false)
    addressTable.Columns.Add(zipColumn)
    // Primary key
    let addressPrimaryKey = Index(addressTable, "AddressPrimaryKey")
    addressPrimaryKey.IndexKeyType <- IndexKeyType.DriPrimaryKey
    addressPrimaryKey.IndexedColumns.Add(IndexedColumn(addressPrimaryKey, "Id"))
    addressPrimaryKey.IsUnique <- true
    addressTable.Indexes.Add(addressPrimaryKey)
    // Foreign key
    let addressPersonId = ForeignKey(addressTable, "AddressPersonIdForeignKey")
    addressPersonId.ReferencedTable <- personTable.Name
    let addressPersonIdColumn = ForeignKeyColumn(addressPersonId, "PersonId", "Id")
    addressPersonId.Columns.Add(addressPersonIdColumn)
    addressTable.ForeignKeys.Add(addressPersonId)
    addressTable.Create()
    
let createTables database =
    createRawJsonTable database
    createJsonTableWithIndexedViews database
    createRelationalTables database
   

