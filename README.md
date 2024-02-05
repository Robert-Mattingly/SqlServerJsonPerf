# SqlServerJsonPerf

A small test project for comparing the performance of JSON vs relational data in SQL Server.

## Getting Started

> # Quick Disclaimer: Run at own risk.
> 
> This project DROPs and CREATEs various SQL Server objects, including a database.  It is also under active development and may not be stable.
> 
> Before handing this (or any application) sa access to your SQL Server, please review the code and ensure it is safe.  I am not responsible for any damage caused by running this code.

You will need dotnet 8.0, access to a SQL Server instance, and sa permissions.

Open the secrets.json and provide the following:

```json
{
  "Sql": {
    "AppLogin": {
      "Username": "...",
      "Password": "..."
    },
    "AdminConnectionString": "..."
  }
}
```

The `AdminConnectionString` should be a connection string that has `sa` permissions.  It will be used to create the database and tables.

The AppLogin is used to create a user with the necessary permissions to run the INSERT and SELECT logic.

Once the secrets are in place, you can run the application.  It will drop any resources from a previous run, create the necessary SQL Server objects, run queries and collect performance data, and then print the results to the console.