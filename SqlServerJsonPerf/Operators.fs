/// <summary>
/// A quick explanation.  Microsoft.SqlServer.Management.Smo's table creation
/// interface annoys me.  Particularly, you're forced to use two lines to add
/// a column or kludge one line.
/// <example>
/// Two lines, one column:
/// <code>
/// let private createRawJsonTable database =
///     let table = Table(database, Constants.RawJsonTableName)
///     let jsonColumn = Column(table, "Json", DataType.NVarCharMax, Nullable = false)
///     table.Columns.Add(jsonColumn)
///     table.Create()
/// </code>
/// One ugly line:
/// <code>
/// let private createRawJsonTable database =
///     let table = Table(database, Constants.RawJsonTableName)
///     table.Columns.Add(Column(table, "Json", DataType.NVarCharMax, Nullable = false))
///     table.Create()
/// </code>
/// </example>
/// Hard pass on that syntax.  Additionally, why am I setting the Parent
/// object for the Column, when I'm adding it to the table?  That's 
/// redundant.  So, I'm sure they had great reasons for the API but I'll
/// write my own.
/// </summary>
module SqlServerJsonPerf.Operators

open Microsoft.SqlServer.Management.Smo

/// <summary>
/// Creates an otherwise default column with the provided args,
/// and adds it to the table.
/// </summary>
/// <param name="table">The table to receive the column.</param>
/// <param name="name">The name of the column.</param>
/// <param name="dataType">The Sql Server <see cref="DataType"/> of the column.</param>
/// <param name="nullable">Whether a row can be stored with null in this column.</param>
let inline (<-|) (table:Table) (name:string, dataType:DataType, nullable:bool) =
    let column = Column(table, name, dataType, Nullable=nullable)
    table.Columns.Add(column)
    table

/// <summary>
/// Creates a computed column with the provided args,
/// and adds it to the table.
/// </summary>
/// <param name="table">The table to receive the column.</param>
/// <param name="name">The name of the column.</param>
/// <param name="dataType">The Sql Server <see cref="DataType"/> of the column.</param>
/// <param name="nullable">Whether a row can be stored with null in this column.</param>
/// <param name="persist">When true, the column will be written to disk.  When false, the column will be computed at query time.</param>
/// <param name="expression">The SQL string which creates the column's value.</param>
let inline (<-/) (table:Table) (name:string, dataType:DataType, nullable:bool, persist:bool, expression:string) =
    let column = Column(table, name, dataType, Nullable=nullable, Computed=true,
                        IsPersisted=persist, ComputedText=expression)
    table.Columns.Add(column)
    table
  
let inline (<-%) (table:Table) (indexName:string, keyType:IndexKeyType, column:string, isUnique:bool) =
    let index = Index(table, indexName)
    index.IndexKeyType <- keyType
    index.IsUnique <- isUnique
    index.IndexedColumns.Add(IndexedColumn(index, column))
    table.Indexes.Add(index)
    table
    
let inline (<-%%) (table:Table) (indexName:string, keyType:IndexKeyType, columnsIncluded:seq<string>, isUnique:bool) =
    let index = Index(table, indexName)
    index.IndexKeyType <- keyType
    index.IsUnique <- isUnique
    columnsIncluded |> Seq.iter (fun c -> index.IndexedColumns.Add(IndexedColumn(index, c)))
    table.Indexes.Add(index)
    table
    
let inline (<-@) (table:Table) (fkName:string, pkTable:string, pkColumn:string, fkColumn:string) =
    let fk = ForeignKey(table, fkName)
    fk.ReferencedTable <- pkTable
    fk.Columns.Add(ForeignKeyColumn(fk, fkColumn, pkColumn))
    table.ForeignKeys.Add(fk)
    table
    
let create (table:Table) =
    table.Create()
    table