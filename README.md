# Migration SQL Server To Oracle

Follow the steps below:
   - Edit the SQLServerToOracleMigration.cmd script with data from the SQL Server and Oracle database connections involved
   - Run it:
       ```terminal
       > SQLServerToOracleMigration.cmd
       ```

       ** After execution, all scripts and logs for creation and population of the tables in the destination will be created with the same data as in the source base. **

The migration of views is still being developed.

Params:
	* sqlhost = SQL Server name or IP address, e.g., sqlhost=sql or sqlhost=myserver
	* sqlport = SQL Server port, ex: sqlport=1433 or sqlport 1433; If not informed, the default value is 1433
	* sqlcatalog = SQL Server database, e.g., sqlcatalog=mydatabase or sqlcatalog mydatabase
	* sqlusr = SQL Server user, e.g., sqlusr=user or sqlusr user
	* sqlpwd = SQL Server password, e.g., sqlpwd=password or sqlpwd password
	* orahost = Oracle server name or IP address, e.g., orahost=192.168.0.2 or orahost 192.168.0.2
	* oraport = Oracle port, e.g., oraport=1521 or oraport 1521; If not informed, the default value is 1521
	* orasid = Oracle Service ID, e.g., orasid=oraserviceId or orasid oraserviceId
	* orausr = Oracle user, e.g., orausr=scott or orausr scott
	* orapwd = Oracle password, e.g., orapwd=tigger or orapwd tigger
	* recriar = true if you want to recreate the tables / false if you don't want to, e.g., recriar=true or recriar true
	* append = true, whether you insert without truncating / false, whether you want to skip the table, e.g., append=true or append true
	* -m = maxRowsToInsert, e.g., -m 100 (maximum of 100 records) 

-------------------------------------------------------------------------------------------------------------------------------------------------

# Migration SQL Server To Oracle

Siga os passos abaixo:
  - Edite o script SQLServerToOracleMigration.cmd com os dados das conexões dos bancos de dados SQL Server e Oracle envolvidos
  - Execute-o : 
      ```terminal
      > SQLServerToOracleMigration.cmd
      ```

      ** Após a execução serão criados todos os scripts e logs de criação e popular das tabelas no destino com os mesmos dados da base origem. **

Ainda está sendo desenvolvido a migração das views.

Params:
	* sqlhost = nome ou ip do servidor do SQL Server, ex: sqlhost=192.168.0.1 ou sqlhost meuservidor
	* sqlport = porta SQL Server, ex: sqlport=1433 ou sqlport 1433; Se não informado, o valor padrão será 1433
	* sqlcatalog = banco de dados do SQL Server, ex: sqlcatalog=meubanco ou sqlcatalog meubanco
	* sqlusr = usuário do SQL Server, ex: sqlusr=usuario ou sqlusr usuario
	* sqlpwd = senha do SQL Server, sqlpwd=senha ou sqlpwd senha
	* orahost = nome ou ip do servidor Oracle, ex: orahost=192.168.0.2 ou orahost 192.168.0.2
	* oraport = porta Oracle, ex: oraport=1521 ou oraport 1521; Se não informado, o valor padrão será 1521
	* orasid = Id do Serviço Oracle, ex: orasid=oraserviceId ou orasid oraserviceId
	* orausr = usuário Oracle, ex: orausr=scott ou orausr scott
	* orapwd = Senha Oracle, ex: orapwd=tigger ou orapwd tigger
	* recriar = true se deseja recirar as tabelas / falso caso não deseje, ex: recriar=true ou recriar true
	* append = true se inserir sem truncar / falso caso queira pular a tabela, ex: append=true ou append true
	* -m = maxRowsToInsert ex: -m 100 (no máximo 100 regitros)
