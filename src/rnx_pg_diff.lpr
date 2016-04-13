program rnx_pg_diff;

{$mode objfpc}{$H+}

uses
  {$IFDEF UNIX}{$IFDEF UseCThreads}
  cthreads,
  {$ENDIF}{$ENDIF}
  Classes, SysUtils, CustApp, dmpgdiff, zcomponent, Interfaces, strutils
  { you can add units after this };

const
  VERSAO_APLIC = 1.05;
  MAX_VETOR = 9999;

type

  { TAtualizaModelo }

  TAtualizaModelo = class(TCustomApplication)
  protected
    procedure DoRun; override;
  private
    modo_debug, modo_verbose, modo_cmd, modo_exec: boolean;
    IP_Master: string;
    IP_Slave: string;
    //--- tabelas, triggers e constraints
    lstMasterTabelas: TStringList;
    lstMasterColunas: TStringList;
    lstMasterTriggers: TStringList;
    lstMasterConstraints: TStringList;
    lstSlaveTabelas: TStringList;
    lstSlaveColunas: TStringList;
    lstSlaveTriggers: TStringList;
    lstSlaveConstraints: TStringList;
    //--- procedures
    lstMasterProc: TStringList;
    lstSlaveProc: TStringList;
    vetMasterFuncoes, vetSlaveFuncoes: array [0..MAX_VETOR] of string;
    //--- views
    lstMasterView: TStringList;
    lstSlaveView: TStringList;
    vetMasterViews, vetSlaveViews: array [0..MAX_VETOR] of string;
    //---
    procedure CarregaTabelas(dest: TStringList);
    procedure CarregaColunas(dest: TStringList);
    procedure CarregaTriggers(dest: TStringList);
    procedure CarregaConstraints(dest: TStringList);
    procedure CarregaFuncoes(dest: TStringList; out vet: array of string);
    procedure CarregaViews(dest: TStringList; out vet: array of string);
    //---
    procedure VerificaTabelas;
    procedure VerificaFuncoes(delimitador: string);
    procedure VerificaTriggers;
    procedure VerificaConstraints;
    procedure VerificaViews;
    function PrimaryKeyTabela(nometab:string):string;
    function VersaoPostgreSQL(out numerica:Currency): string;
  public
    pg_username, pg_database, pg_schema, pg_password: string;
    constructor Create(TheOwner: TComponent); override;
    destructor Destroy; override;
    procedure WriteHelp; virtual;
  end;

{ TAtualizaModelo }

procedure TAtualizaModelo.DoRun;
var resp, ErrorMsg: String;
  versao_pg: Currency;
begin
  DefaultFormatSettings.DecimalSeparator := '.';
  WriteLn(Format('-- Rednaxel PostgreSQL Diff Tool - v%.2f',[VERSAO_APLIC]));

  // quick check parameters
  ErrorMsg:=CheckOptions('hm:vql:xcu:p:d:s:','help master: verbose queries local: exec commands user: password: database: schema:');
  if ErrorMsg<>'' then
  begin
    ShowException(Exception.Create(ErrorMsg));
    Terminate;
    Exit;
  end;

  // parse parameters
  IP_Master := GetOptionValue('m','master');
  if HasOption('h','help') or (IP_Master = '') then
  begin
    WriteHelp;
    Terminate;
    Exit;
  end;
  IP_Slave := GetOptionValue('l','local');
  modo_debug := HasOption('q','queries');
  modo_verbose := HasOption('v','verbose');
  modo_exec := HasOption('x','exec');
  modo_cmd := HasOption('c','commands');

  pg_username := GetOptionValue('u','user');
  dtmAtualizaMod.ZConnection1.User := pg_username;

  pg_password := GetOptionValue('p','password');
  if pg_password <> '' then
    dtmAtualizaMod.ZConnection1.Password := pg_password;

  pg_database := GetOptionValue('d','database');
  dtmAtualizaMod.ZConnection1.Database := pg_database;

  pg_schema := GetOptionValue('s','schema');
  if pg_schema = '' then
    pg_schema := 'public';

  //--- conecta com o servidor slave
  if modo_verbose then
  begin
    WriteLn('-- ****************************');
    WriteLn('-- Connecting to slave...');
  end;
  if not dtmAtualizaMod.ConectaBanco(dtmAtualizaMod.ZConnection1, IP_Slave) then
  begin
    WriteLn(dtmAtualizaMod.MensagemConexao);
    Terminate;
    Exit;
  end;
  resp := VersaoPostgreSQL(versao_pg);
  if modo_verbose then
  begin
    WriteLn('-- SLAVE='+dtmAtualizaMod.MensagemConexao);
    WriteLn('-- PostgreSQL '+resp);
  end;

  //--- verifica versao do PostgreSQL
  if versao_pg < 9.0 then
  begin
    WriteLn(Format('PostgreSQL version %.1f below 9.0 (mínimum).',[versao_pg]));
    Terminate;
    Exit;
  end;

  CarregaTabelas(lstSlaveTabelas);
  CarregaColunas(lstSlaveColunas);
  CarregaTriggers(lstSlaveTriggers);
  CarregaConstraints(lstSlaveConstraints);
  CarregaFuncoes(lstSlaveProc, vetSlaveFuncoes);
  CarregaViews(lstSlaveView, vetSlaveViews);

  //--- conecta com o servidor master
  if modo_verbose then
  begin
    WriteLn('-- ****************************');
    WriteLn('-- Connecting to master...');
  end;
  if not dtmAtualizaMod.ConectaBanco(dtmAtualizaMod.ZConnection1, IP_Master) then
  begin
    WriteLn(dtmAtualizaMod.MensagemConexao);
    Terminate;
    Exit;
  end;
  resp := VersaoPostgreSQL(versao_pg);
  if modo_verbose then
  begin
    WriteLn('-- MASTER='+dtmAtualizaMod.MensagemConexao);
    WriteLn('-- PostgreSQL '+resp);
  end;

  CarregaTabelas(lstMasterTabelas);
  CarregaColunas(lstMasterColunas);
  CarregaTriggers(lstMasterTriggers);
  CarregaConstraints(lstMasterConstraints);
  CarregaFuncoes(lstMasterProc, vetMasterFuncoes);
  CarregaViews(lstMasterView, vetMasterViews);

  if modo_verbose then
  begin
    WriteLn('-- ****************************');
    WriteLn('-- Disconnecting...');
  end;
  dtmAtualizaMod.ZConnection1.Connected := false;

  if modo_exec then
  begin
    WriteLn(Format('-- EXECUTING COMMANDS (on %s)...',[IP_Slave]));
    if not dtmAtualizaMod.ConectaBanco(dtmAtualizaMod.ZConnection1, IP_Slave) then
    begin
      WriteLn(dtmAtualizaMod.MensagemConexao);
      Terminate;
      Exit;
    end;
    resp := dtmAtualizaMod.GravaSQL('BEGIN TRANSACTION;');
    if resp <> 'OK' then
    begin
      WriteLn(resp);
      Terminate;
      Exit;
    end;
    if modo_verbose then
    begin
      WriteLn('-- SLAVE='+dtmAtualizaMod.MensagemConexao);
      WriteLn('-- BEGIN TRANSACTION');
    end;
  end;

  //--- compara
  VerificaTabelas;
  VerificaConstraints;
  VerificaFuncoes('$function$');
  VerificaTriggers;
  VerificaViews;

  if modo_exec then
  begin
    WriteLn('-- COMMIT...');
    resp := dtmAtualizaMod.GravaSQL('COMMIT;');
    if resp <> 'OK' then
    begin
      WriteLn(resp);
      Terminate;
      Exit;
    end;
  end;

  if modo_verbose then
    WriteLn('-- OK');

  // stop program loop
  Terminate;
end;

procedure TAtualizaModelo.CarregaTabelas(dest: TStringList);
var cmd_sql: string;
begin
  cmd_sql := Format('SELECT table_name FROM information_schema.tables'+
    ' WHERE table_catalog = %s AND table_schema = %s AND table_type <> %s'+
    ' ORDER BY 1', [QuotedStr(pg_database), QuotedStr(pg_schema), QuotedStr('VIEW')]);
  if modo_debug then
    WriteLn('/*'+sLineBreak+cmd_sql+sLineBreak+'*/');
  dtmAtualizaMod.ExecutaSqlRetornaStringList(dtmAtualizaMod.ZReadOnlyQuery1, cmd_sql, dest, true);
  if modo_verbose then
    WriteLn(Format('-- Tables: %d',[dest.Count]));
end;

procedure TAtualizaModelo.CarregaColunas(dest: TStringList);
var cmd_sql: string;
begin
  cmd_sql := Format('SELECT foo.table_name, ordinal_position, column_name, data_type,'+
  ' CASE WHEN is_nullable = ''NO'' THEN '' NOT NULL'' END as naonulo, column_default,'+
  'character_maximum_length, numeric_precision, numeric_scale, datetime_precision FROM ('+
  'SELECT table_name FROM information_schema.tables WHERE table_catalog = %s'+
  ' AND table_schema = %s AND table_type <> %s) as foo'+
  ' JOIN information_schema.columns c ON (foo.table_name = c.table_name)'+
  ' ORDER BY 1, 2', [QuotedStr(pg_database), QuotedStr(pg_schema), QuotedStr('VIEW')]);
  if modo_debug then
    WriteLn('/*'+sLineBreak+cmd_sql+sLineBreak+'*/');
  dtmAtualizaMod.ExecutaSqlRetornaStringList(dtmAtualizaMod.ZReadOnlyQuery1, cmd_sql, dest);
  if modo_verbose then
    WriteLn(Format('-- Columns: %d',[dest.Count]));
end;

procedure TAtualizaModelo.CarregaTriggers(dest: TStringList);
var cmd_sql: string;
begin
  cmd_sql := 'SELECT r.relname, t.tgname, md5(pg_catalog.pg_get_triggerdef(t.oid)) as md5def,'+
    Format(' pg_catalog.pg_get_triggerdef(t.oid) as definicao FROM pg_catalog.pg_class r'+
    ' JOIN pg_catalog.pg_trigger t ON r.oid = t.tgrelid WHERE r.relkind = ''r'' AND NOT t.tgisinternal'+
    ' AND r.relname IN (SELECT tablename FROM pg_tables WHERE schemaname = %s) order by relname, tgname',
    [QuotedStr(pg_schema)]);
  if modo_debug then
    WriteLn('/*'+sLineBreak+cmd_sql+sLineBreak+'*/');
  dtmAtualizaMod.ExecutaSqlRetornaStringList(dtmAtualizaMod.ZReadOnlyQuery1, cmd_sql, dest);
  if modo_verbose then
    WriteLn(Format('-- Triggers: %d',[dest.Count]));
end;

procedure TAtualizaModelo.CarregaFuncoes(dest: TStringList; out vet: array of string);
var i, ret: integer;
  oid: Int64;
  cmd_sql, parte1, parte2: string;
begin
  parte1 := 'SELECT proname, pg_get_function_arguments(p.oid), pg_get_function_result(p.oid),'+
    ' cast(p.oid as text), l.lanname, md5(prosrc) as md5sum';
  parte2 := ' FROM pg_proc p LEFT OUTER JOIN pg_language l ON (prolang = l.oid)' +
    Format(' WHERE proowner = (SELECT usesysid FROM pg_user WHERE usename = %s)'+
    ' AND pronamespace = (SELECT oid FROM pg_namespace WHERE nspname = %s)',
    [QuotedStr(pg_username), QuotedStr(pg_schema)]);
  cmd_sql := parte1 + parte2 + ' order by 1, 2';
  if modo_debug then
    WriteLn('/*'+sLineBreak+cmd_sql+sLineBreak+'*/');
  ret := dtmAtualizaMod.ExecutaSqlRetornaStringList(dtmAtualizaMod.ZReadOnlyQuery1, cmd_sql, dest);
  if ret < 0 then
  begin
    WriteLn(dtmAtualizaMod.TextoExcecao);
    Exit;
  end;
  if modo_verbose then
    WriteLn(Format('-- Procedures: %d',[dest.Count]));
  if modo_debug then
    WriteLn(dest.Text);
  parte1 := 'SELECT prosrc';
  for i:=0 to dest.Count-1 do
  begin
    oid := StrToInt64Def(ExtractDelimited(4,dest[i],['|']),0);
    if oid = 0 then
      WriteLn(dest[i]);
    cmd_sql := parte1 + parte2 + ' AND p.oid = ' + IntToStr(oid);
    if modo_debug then
      WriteLn('/*'+sLineBreak+cmd_sql+sLineBreak+'*/');
    vet[i] := dtmAtualizaMod.ExecutaSqlRetornaString(dtmAtualizaMod.ZReadOnlyQuery1, cmd_sql);
  end;
end;

procedure TAtualizaModelo.CarregaConstraints(dest: TStringList);
var cmd_sql: string;
begin
  cmd_sql := Format('SELECT relname, conname, md5(pg_catalog.pg_get_constraintdef(r.oid, true)) as md5con,'+
    ' pg_catalog.pg_get_constraintdef(r.oid, true) FROM pg_constraint r'+
    ' JOIN pg_class c ON (c.oid = conrelid) WHERE connamespace IN '+
    '(SELECT oid FROM pg_namespace WHERE nspname = %s) order by 1, 2', [QuotedStr(pg_schema)]);
  if modo_debug then
    WriteLn('/*'+sLineBreak+cmd_sql+sLineBreak+'*/');
  dtmAtualizaMod.ExecutaSqlRetornaStringList(dtmAtualizaMod.ZReadOnlyQuery1, cmd_sql, dest);
  if modo_verbose then
    WriteLn(Format('-- Constraints: %d',[dest.Count]));
end;

procedure TAtualizaModelo.CarregaViews(dest: TStringList; out
  vet: array of string);
var i, ret: integer;
  cmd_sql, nomeview: string;
begin
  cmd_sql := Format('SELECT viewname, md5(definition) as md5def FROM pg_views WHERE schemaname = %s'+
    ' AND viewowner = %s ORDER BY 1', [QuotedStr(pg_schema), QuotedStr(pg_username)]);
  if modo_debug then
    WriteLn('/*'+sLineBreak+cmd_sql+sLineBreak+'*/');
  ret := dtmAtualizaMod.ExecutaSqlRetornaStringList(dtmAtualizaMod.ZReadOnlyQuery1, cmd_sql, dest);
  if ret < 0 then
  begin
    WriteLn(dtmAtualizaMod.TextoExcecao);
    Exit;
  end;
  if modo_verbose then
    WriteLn(Format('-- Views: %d',[dest.Count]));
  for i:=0 to dest.Count-1 do
  begin
    nomeview := ExtractDelimited(1,dest[i],['|']);
    cmd_sql := Format('SELECT definition FROM pg_views WHERE viewname = %s AND schemaname = %s'+
      ' AND viewowner = %s', [QuotedStr(nomeview), QuotedStr(pg_schema), QuotedStr(pg_username)]);
    vet[i] := dtmAtualizaMod.ExecutaSqlRetornaString(dtmAtualizaMod.ZReadOnlyQuery1, cmd_sql);
  end;
end;

procedure TAtualizaModelo.VerificaTabelas;
var i, j, k, idx, idxm, ordm, ords: integer;
  tabmast, tabcolm, ulttabk, tabcols, colmast, colslav: string;
  ddl_tipo, ddl_notnull, ddl_default, ddl_pk: string;
  ddl_charmax, ddl_numprec, ddl_numscale, ddl_dtprec: integer;
  resp, cmd_sql: string;
begin
  idxm := 0;
  k := 0;
  for i:=0 to lstMasterTabelas.Count-1 do
  begin
    tabmast := ExtractDelimited(1,lstMasterTabelas[i],['|']);
    idx := lstSlaveTabelas.IndexOf(tabmast);
    if idx < 0 then
    begin
      WriteLn(Format('-- New table %s',[tabmast]));
      cmd_sql := Format('CREATE TABLE %s (',[tabmast]) + sLineBreak;
      for j:=idxm to lstMasterColunas.Count-1 do
      begin
        tabcolm := ExtractDelimited(1,lstMasterColunas[j],['|']);
        if tabcolm <> tabmast then
        begin
          idxm := j;
          Break;
        end;
        if j > idxm then
          cmd_sql := cmd_sql + ',' + sLineBreak;
        //--- carrega campos
        colmast := ExtractDelimited(3,lstMasterColunas[j],['|']);
        ddl_tipo := ExtractDelimited(4,lstMasterColunas[j],['|']);
        ddl_notnull := ExtractDelimited(5,lstMasterColunas[j],['|']);
        ddl_default := ExtractDelimited(6,lstMasterColunas[j],['|']);
        ddl_charmax := StrToIntDef(ExtractDelimited(7,lstMasterColunas[j],['|']),-1);
        ddl_numprec := StrToIntDef(ExtractDelimited(8,lstMasterColunas[j],['|']),-1);
        ddl_numscale := StrToIntDef(ExtractDelimited(9,lstMasterColunas[j],['|']),-1);
        ddl_dtprec := StrToIntDef(ExtractDelimited(10,lstMasterColunas[j],['|']),-1);
        //--- monta comando
        cmd_sql := cmd_sql + Format('  %s %s',[colmast, ddl_tipo]);
        if ddl_charmax >= 0 then
          cmd_sql := cmd_sql + Format('(%d)',[ddl_charmax]);
        if (ddl_tipo = 'numeric') and (ddl_numprec >= 0) then
        begin
          if ddl_numscale < 0 then
            cmd_sql := cmd_sql + Format('(%d)',[ddl_numprec])
          else
            cmd_sql := cmd_sql + Format('(%d,%d)',[ddl_numprec, ddl_numscale]);
        end;
        if (ddl_tipo <> 'date') and (ddl_dtprec >= 0) then
          cmd_sql := cmd_sql + Format('(%d)',[ddl_dtprec]);
        //--- modificadores
        cmd_sql := cmd_sql + ddl_notnull;
        if ddl_default <> '' then
          cmd_sql := cmd_sql + ' DEFAULT '+ddl_default;
      end;
      ddl_pk := PrimaryKeyTabela(tabmast);
      if ddl_pk <> '' then
      begin
        cmd_sql := cmd_sql + ',' + sLineBreak;
        cmd_sql := cmd_sql + Format('  PRIMARY KEY (%s)',[ddl_pk]);
      end;
      cmd_sql := cmd_sql + sLineBreak;
      cmd_sql := cmd_sql + '  );';
      if modo_cmd then
        WriteLn(cmd_sql);
      if modo_exec then
      begin
        resp := dtmAtualizaMod.GravaSQL(cmd_sql);
        if resp <> 'OK' then
        begin
          WriteLn(resp);
          Terminate;
          Exit;
        end;
      end;
      Continue;
    end;
    for j:=idxm to lstMasterColunas.Count-1 do
    begin
      tabcolm := ExtractDelimited(1,lstMasterColunas[j],['|']);
      if tabcolm <> tabmast then
      begin
        idxm := j;
        Break;
      end;
      tabcols := ExtractDelimited(1,lstSlaveColunas[k],['|']);
      if tabcols <> tabmast then
      begin
        WriteLn(Format('-- Table %s does not exist on MASTER.',[tabcols]));
        ulttabk := tabcols;
        repeat
          Inc(k);
          tabcols := ExtractDelimited(1,lstSlaveColunas[k],['|']);
          if (tabcols <> tabmast) and (tabcols <> ulttabk) then
          begin
            WriteLn(Format('-- Table %s does not exist on MASTER.',[tabcols]));
            ulttabk := tabcols;
          end;
        until (tabcols = tabmast) or (k >= lstSlaveColunas.Count-1);
        if k >= lstSlaveColunas.Count-1 then Continue;
      end;
      colmast := ExtractDelimited(3,lstMasterColunas[j],['|']);
      colslav := ExtractDelimited(3,lstSlaveColunas[k],['|']);
      if colmast <> colslav then
      begin
        ordm := StrToIntDef(ExtractDelimited(2,lstMasterColunas[j],['|']),0);
        ords := StrToIntDef(ExtractDelimited(2,lstMasterColunas[k-1],['|']),0);
        if ordm-1 = ords then
        begin
          ddl_tipo := ExtractDelimited(4,lstMasterColunas[j],['|']);
          WriteLn(Format('-- New column %s on table %s',[ddl_tipo, tabmast]));
          cmd_sql := Format('ALTER TABLE %s ADD COLUMN %s %s;',[tabmast, ddl_tipo]);
          if modo_cmd then
            WriteLn(cmd_sql);
          if modo_exec then
          begin
            resp := dtmAtualizaMod.GravaSQL(cmd_sql);
            if resp <> 'OK' then
            begin
              WriteLn(resp);
              Terminate;
              Exit;
            end;
          end;
          Break;
        end;
      end;
      Inc(k);
    end;
  end;
end;

procedure TAtualizaModelo.VerificaFuncoes(delimitador: string);
var i, j: integer;
  mastfunc, mastargs: string;
  slavfunc, slavargs: string;
  tiporet, linguagem: string;
  mastmd5, slavmd5: string;
  achou, atualizar: boolean;
  resp, cmd_sql: string;
begin
  for i:=0 to lstMasterProc.Count-1 do
  begin
    mastfunc := ExtractDelimited(1,lstMasterProc[i],['|']);
    mastargs := ExtractDelimited(2,lstMasterProc[i],['|']);
    tiporet := ExtractDelimited(3,lstMasterProc[i],['|']);
    linguagem := ExtractDelimited(5,lstMasterProc[i],['|']);
    mastmd5 := ExtractDelimited(6,lstMasterProc[i],['|']);
    achou := false;
    atualizar := false;
    for j:=0 to lstSlaveProc.Count-1 do
    begin
      slavfunc := ExtractDelimited(1,lstSlaveProc[j],['|']);
      slavargs := ExtractDelimited(2,lstSlaveProc[j],['|']);
      if (slavfunc = mastfunc) and (slavargs = mastargs) then
      begin
        achou := true;
        Break;
      end;
    end;
    if not achou then
    begin
      atualizar := true;
      WriteLn(Format('-- New procedure %s(%s)',[mastfunc, mastargs]));
    end
    else
    begin
      slavmd5 := ExtractDelimited(6,lstSlaveProc[j],['|']);
      if mastmd5 <> slavmd5 then
      begin
        atualizar := true;
        WriteLn(Format('-- Replacing procedure %s(%s)',[mastfunc, mastargs]));
      end;
    end;
    if atualizar then
    begin
      cmd_sql := Format('CREATE OR REPLACE FUNCTION %s (%s) RETURNS %s', [mastfunc, mastargs, tiporet]);
      cmd_sql := cmd_sql + Format('  LANGUAGE %s AS %s%s%s;',
        [linguagem, delimitador, vetMasterFuncoes[i], delimitador]);
      if modo_cmd then
        WriteLn(cmd_sql);
      if modo_exec then
      begin
        resp := dtmAtualizaMod.GravaSQL(cmd_sql);
        if resp <> 'OK' then
        begin
          WriteLn(resp);
          Terminate;
          Exit;
        end;
      end;
    end;
  end;
end;

procedure TAtualizaModelo.VerificaTriggers;
var i, j: integer;
  mastable, masttrig, slavtabl, slavtrig, mastmd5, slavmd5, sqltrig: string;
  achou, atualizar: boolean;
  resp, cmd_sql: string;
begin
  for i:=0 to lstMasterTriggers.Count-1 do
  begin
    mastable := ExtractDelimited(1,lstMasterTriggers[i],['|']);
    masttrig := ExtractDelimited(2,lstMasterTriggers[i],['|']);
    mastmd5 := ExtractDelimited(3,lstMasterTriggers[i],['|']);
    sqltrig := ExtractDelimited(4,lstMasterTriggers[i],['|']);
    achou := false;
    atualizar := false;
    for j:=0 to lstSlaveTriggers.Count-1 do
    begin
      slavtabl := ExtractDelimited(1,lstSlaveTriggers[j],['|']);
      slavtrig := ExtractDelimited(2,lstSlaveTriggers[j],['|']);
      if (slavtabl = mastable) and (slavtrig = masttrig) then
      begin
        achou := true;
        Break;
      end;
    end;
    if not achou then
    begin
      atualizar := true;
      WriteLn(Format('-- New trigger %s em %s',[masttrig, mastable]));
    end
    else
    begin
      slavmd5 := ExtractDelimited(3,lstSlaveTriggers[j],['|']);
      if mastmd5 <> slavmd5 then
      begin
        WriteLn('-- (removing trigger '+masttrig+')');
        atualizar := true;
        cmd_sql := Format('DROP TRIGGER %s ON %s;',[masttrig, mastable]);
        if modo_cmd then
          WriteLn(cmd_sql);
        if modo_exec then
        begin
          resp := dtmAtualizaMod.GravaSQL(cmd_sql);
          if resp <> 'OK' then
          begin
            WriteLn(resp);
            Terminate;
            Exit;
          end;
        end;
      end;
    end;
    if atualizar then
    begin
      cmd_sql := Format('%s;', [sqltrig]);
      if modo_cmd then
        WriteLn(cmd_sql);
      if modo_exec then
      begin
        resp := dtmAtualizaMod.GravaSQL(cmd_sql);
        if resp <> 'OK' then
        begin
          WriteLn(resp);
          Terminate;
          Exit;
        end;
      end;
    end;
  end;
end;

procedure TAtualizaModelo.VerificaConstraints;
var i, j: integer;
  mastable, mastcon, slavtabl, slavcon, mastmd5, slavmd5, sqlcon: string;
  achou, atualizar: boolean;
begin
  for i:=0 to lstMasterConstraints.Count-1 do
  begin
    mastable := ExtractDelimited(1,lstMasterConstraints[i],['|']);
    mastcon := ExtractDelimited(2,lstMasterConstraints[i],['|']);
    mastmd5 := ExtractDelimited(3,lstMasterConstraints[i],['|']);
    sqlcon := ExtractDelimited(4,lstMasterConstraints[i],['|']);
    achou := false;
    atualizar := false;
    for j:=0 to lstSlaveConstraints.Count-1 do
    begin
      slavtabl := ExtractDelimited(1,lstSlaveConstraints[j],['|']);
      slavcon := ExtractDelimited(2,lstSlaveConstraints[j],['|']);
      if (slavtabl = mastable) and (slavcon = mastcon) then
      begin
        achou := true;
        Break;
      end;
    end;
    if LeftStr(mastcon,1) = '$' then
      mastcon := Format('"%s"',[mastcon]);
    if not achou then
    begin
      atualizar := true;
      if modo_verbose then
        WriteLn(Format('-- Creating constraint %s em %s',[mastcon, mastable]));
    end
    else
    begin
      slavmd5 := ExtractDelimited(3,lstSlaveConstraints[j],['|']);
      if mastmd5 <> slavmd5 then
      begin
        atualizar := true;
        WriteLn(Format('ALTER TABLE %s DROP CONSTRAINT %s;',[mastable, mastcon]));
      end;
    end;
    if atualizar then
    begin
      WriteLn(Format('ALTER TABLE %s ADD CONSTRAINT %s %s;',[mastable, mastcon, sqlcon]));
    end;
  end;
end;

procedure TAtualizaModelo.VerificaViews;
var i, j: integer;
  mastview, slavview: string;
  mastmd5, slavmd5: string;
  achou, atualizar: boolean;
  resp, cmd_sql: string;
begin
  for i:=0 to lstMasterView.Count-1 do
  begin
    mastview := ExtractDelimited(1,lstMasterView[i],['|']);
    mastmd5 := ExtractDelimited(2,lstMasterView[i],['|']);
    achou := false;
    atualizar := false;
    for j:=0 to lstSlaveView.Count-1 do
    begin
      slavview := ExtractDelimited(1,lstSlaveView[j],['|']);
      if slavview = mastview then
      begin
        achou := true;
        Break;
      end;
    end;
    if not achou then
    begin
      atualizar := true;
      WriteLn(Format('-- New view %s',[mastview]));
    end
    else
    begin
      slavmd5 := ExtractDelimited(2,lstSlaveView[j],['|']);
      if mastmd5 <> slavmd5 then
      begin
        atualizar := true;
        WriteLn(Format('-- Replacing view %s',[mastview]));
      end;
    end;
    if atualizar then
    begin
      cmd_sql := Format('CREATE OR REPLACE VIEW %s AS', [mastview]) + sLineBreak;
      cmd_sql := cmd_sql + Format('%s;', [vetMasterViews[i]]);
      if modo_cmd then
        WriteLn(cmd_sql);
      if modo_exec then
      begin
        resp := dtmAtualizaMod.GravaSQL(cmd_sql);
        if resp <> 'OK' then
        begin
          WriteLn(resp);
          Terminate;
          Exit;
        end;
      end;
    end;
  end;
end;

function TAtualizaModelo.PrimaryKeyTabela(nometab: string): string;
var cmd_sql, resp: string;
begin
  cmd_sql := Format('SELECT array_to_string(array(select attname from pg_attribute'+
    ' where attrelid = conrelid and attnum = ANY(conkey)'+
    ' order by find_array_element(attnum,conkey)),'','') as colunas'+
    ' FROM pg_constraint JOIN pg_class c ON (c.oid = conrelid)'+
    ' WHERE connamespace IN (SELECT oid FROM pg_namespace'+
    ' WHERE nspname = %s) AND contype = ''p'' AND relname = %s',
    [QuotedStr(pg_schema),QuotedStr(nometab)]);
  if modo_debug then
    WriteLn(sLineBreak+'/*'+sLineBreak+cmd_sql+sLineBreak+'*/');
  resp := dtmAtualizaMod.ExecutaSqlRetornaString(dtmAtualizaMod.ZReadOnlyQuery1, cmd_sql);
  if modo_debug then
    WriteLn('resp='+resp);
  Result := resp;
end;

function TAtualizaModelo.VersaoPostgreSQL(out numerica:Currency): string;
var resp, cmd_sql: string;
  versao_num: integer;
begin
  cmd_sql := 'SHOW server_version';
  Result := dtmAtualizaMod.ExecutaSqlRetornaString(dtmAtualizaMod.ZReadOnlyQuery1, cmd_sql);
  cmd_sql := 'SHOW server_version_num';
  resp := dtmAtualizaMod.ExecutaSqlRetornaString(dtmAtualizaMod.ZReadOnlyQuery1, cmd_sql);
  versao_num := StrToIntDef(resp,0) div 100;
  numerica := versao_num / 100.0;
end;

constructor TAtualizaModelo.Create(TheOwner: TComponent);
begin
  inherited Create(TheOwner);
  StopOnException:=True;
  //---
  lstMasterTabelas := TStringList.Create;
  lstMasterColunas := TStringList.Create;
  lstMasterTriggers := TStringList.Create;
  lstMasterConstraints := TStringList.Create;
  //---
  lstSlaveTabelas := TStringList.Create;
  lstSlaveColunas := TStringList.Create;
  lstSlaveTriggers := TStringList.Create;
  lstSlaveConstraints := TStringList.Create;
  //---
  lstMasterProc := TStringList.Create;
  lstSlaveProc := TStringList.Create;
  //---
  lstMasterView := TStringList.Create;
  lstSlaveView := TStringList.Create;
end;

destructor TAtualizaModelo.Destroy;
begin
  lstMasterTabelas.Free;
  lstMasterColunas.Free;
  lstMasterTriggers.Free;
  lstMasterConstraints.Free;
  lstSlaveTabelas.Free;
  lstSlaveColunas.Free;
  lstSlaveTriggers.Free;
  lstSlaveConstraints.Free;
  lstMasterProc.Free;
  lstSlaveProc.Free;
  lstMasterView.Free;
  lstSlaveView.Free;
  inherited Destroy;
end;

procedure TAtualizaModelo.WriteHelp;
begin
  { add your help code here }
  WriteLn('Syntax: '+ExtractFileName(ApplicationName)+' -m IP [options]');
  WriteLn('  -m IP, --master=IP    Master server IP address.');
  WriteLn('Options:');
  WriteLn('  -u, --username        Username.');
  WriteLn('  -p, --password        Password.');
  WriteLn('  -d, --database        Database.');
  WriteLn('  -s, --schema          Schema (default=public).');
  WriteLn('  -h, --help            Prints this message.');
  WriteLn('  -q, --queries         Prints internal SQL queries.');
  WriteLn('  -v, --verbose         Prints detailed output.');
  WriteLn('  -x, --exec            Executes the DDL commands on slave.');
end;

var
  Application: TAtualizaModelo;
begin
  Application:=TAtualizaModelo.Create(nil);
  dtmAtualizaMod := TdtmAtualizaMod.Create(nil);
  Application.Run;
  Application.Free;
end.

