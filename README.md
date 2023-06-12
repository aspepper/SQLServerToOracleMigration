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
* sqlhost = SQL Server name or IP address, e.g. **sqlhost**=_myserver_ or **sqlhost** _myserver_
* sqlport = SQL Server port, e.g. **sqlport**=_1433_ or **sqlport** _1433_; If not informed, the default value is 1433
* sqlcatalog = SQL Server database, e.g. **sqlcatalog**=_mydatabase_ or **sqlcatalog** _mydatabase_
* sqlusr = SQL Server user, e.g. **sqlusr**=_user_ or **sqlusr** _user_
* sqlpwd = SQL Server password, e.g. **sqlpwd**=_password_ or **sqlpwd** _password_
* orahost = Oracle server name or IP address, e.g. **orahost**=_192.168.0.2_ or **orahost** _192.168.0.2_
* oraport = Oracle port, e.g. **oraport**=_1521_ or **oraport** _1521_; If not informed, the default value is 1521
* orasid = Oracle Service ID, e.g. **orasid**=_oraserviceId_ or **orasid** _oraserviceId_
* orausr = Oracle user, e.g. **orausr**=_scott_ or **orausr** _scott_
* orapwd = Oracle password, e.g. **orapwd**=_tigger_ or **orapwd** _tigger_
* recriar = _true_ if you want to recreate the tables / _false_ if you don't want to, e.g. **recriar**=_true_ or **recriar** _true_
* append = _true_, whether you insert without truncating / _false_, whether you want to skip the table, e.g. **append**=_true_ or **append** _true_
* -m = maxRowsToInsert, e.g. **-m** _100_ (maximum of 100 records) 

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
* sqlhost = nome ou ip do servidor do SQL Server, ex: **sqlhost**=_meuservidor_ ou **sqlhost** _meuservidor_
* sqlport = porta SQL Server, ex: **sqlport**=_1433_ ou **sqlport** 1433; Se não informado, o valor padrão será 1433
* sqlcatalog = banco de dados do SQL Server, ex: **sqlcatalog**=_meubanco_ ou **sqlcatalog** _meubanco_
* sqlusr = usuário do SQL Server, ex: **sqlusr**=_usuario_ ou **sqlusr** _usuario_
* sqlpwd = senha do SQL Server, ex: **sqlpwd**=_senha_ ou **sqlpwd** _senha_
* orahost = nome ou ip do servidor Oracle, ex: **orahost**=_192.168.0.2_ ou **orahost** _192.168.0.2_
* oraport = porta Oracle, ex: **oraport**=_1521_ ou **oraport** _1521_; Se não informado, o valor padrão será 1521
* orasid = Id do Serviço Oracle, ex: **orasid**=_oraserviceId_ ou **orasid** _oraserviceId_
* orausr = usuário Oracle, ex: **orausr**=_scott_ ou **orausr** _scott_
* orapwd = Senha Oracle, ex: **orapwd**=_tigger_ ou **orapwd** _tigger_
* recriar = _true_ se deseja recirar as tabelas / _falso_ caso não deseje, ex: **recriar**=_true_ ou **recriar** _true_
* append = _true_ se inserir sem truncar / _falso_ caso queira pular a tabela, ex: **append**=_true_ ou **append** _true_
* -m = maxRowsToInsert ex: **-m** _100_ (no máximo 100 regitros)
