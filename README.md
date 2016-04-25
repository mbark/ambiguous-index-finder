# ambiguous-index-finder
The tool developer for my master thesis, which in turn can be found [https://github.com/mbark/master_thesis](here). The focus of the thesis is the performance of query optimizers' and more specifically how much of an effect the sample size used to estimate statistics of the tables affects the index selection.

This tool allows analysis of a database by allowing the user to resample data, find the resulting query plan and save it. Following this the tool allows the resulting data to be parsed, providing all index selections used in all plans, and then analyzed to see the total number of different access methods used to access the same relation.

The following sections will give an introduction to the structure of the project, how to run it and how to read the resulting data.

## Structure of the project
The project consists of three main components: the source files, the saved results and the resources. Each of these are covered in the following three sections.

### src
The project is structured into four different classes:
- core.clj, which is the main file, handling things such as chaining steps together, reading previous results and saving them;
- generator.clj, which is the part that handles the JDBC-connection, setting the sample sizes and finding query plans;
- parser.clj, which takes the resulting output of the generator and finds all access methods for each relation in the query;
- and analyzer.clj, which finds all unique access methods used for each relation.

In a typical execution core.clj will handle the flow of the program, generating, parsing and analyzing and saving the result of each step to file for later analysis.

### Saved files
The files are saved in three different folders:
- /plans holds the plans generated;
- /parses holds the parsed plans;
- /analyzes holds the analysis of a parsed plan.

In order to ensure the files saved for each are unique they are formated as {xxx}-{currentTimeMillis} where xxx are three random lowercase letters used to allow easier identification and currentTimeMillis is the current time in milliseconds, used to ensure no collisions between files occur. The file names are generated randomly for each new execution, and as such many files can correspond to the same content.

### Resources
The queries used and the connection details for the databases are saved in the resources folder. The structure of the resources folder is as follows:
- /resources/config holds the configuration files used, specifying things such as database connection info;
- /resources/queries holds the queries used for execution.

Every query must have a corresponding query for the ANALYZEs that need to be done. The name of this file should be queryid-analyze.sql, where queryid is the name of the corresponding query. Queries are not treated differently for different databases, so different query files may be needed for different databases depending on syntax.

Below is an example of a configuration file.
```
{:mysql
 {:classname "com.mysql.jdbc.Driver"
  :subprotocol "mysql"
  :subname "//hostname:port/db"
  :user "user"
  :password "password"}
 :postgresql
 {:classname "org.postgresql.Driver"
  :subprotocol "postgresql"
  :subname "//localhost:5432/db"}}
```

## Usage
The tool is most easily used via leiningen, an example execution can be seen below.
```
    lein run --steps='generate parse analyze' --query=queryid --repetitions=100 --samplesizes='10 100' --database=postgresql
```

With these parameters the tool will generate, parse and analyze the query. The query evaluated is queryid, which should have two corresponding files in the resources/queries directory: the query itself called queryid.sql and the analyze query called queryid-analyze.sql.
The tables used by the query will be analyzed 100 times and the query plan found, for each of the sample sizes 10 and 100. The database that this evaluation is done against is the one described by the identifier postgresql.

Below is given a more in-detail description of each of the parameters and their possible values.

### steps
The steps parameter describe the steps to run, valid parameters are: generate, parse and analyze. If the generate step is not called the first parameter given to lein run should be the file to do the parsing or analysis of.

It is not possible to do a generate and analyze only (that is using `steps='generate analyze'`), if this is attempted only the generate step will be executed.

### query
The query parameter identifies which query to evaluate. The query should be a normal query, the necessary parameters to add such as "DESCRIBE" will be added automatically. It is important that there exists a corresponding file describing how to analyze the tables used by the query.

### repetitions
The repetitions parameter described how many times each evaluation is repeated per sample size. This should be sufficiently large so that all reasonable possible execution paths are covered.

### samplesizes
The samplesizes parameter is a list of the sample sizes for which to evaluate the query. The values should usually be given in ascending order, although it is not strictly necessary.

### database
The database parameter specifies which database to evaluate against and the name should correspond to key in the configuration file in resources/config. See the example of a configuration file above for an example.

## License

Copyright © 2016 Martin Barksten

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
