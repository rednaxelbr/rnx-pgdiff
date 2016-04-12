unit dmpgdiff;

{$mode objfpc}{$H+}

interface

uses
  Classes, SysUtils, ZConnection, ZDataset, variants;

const
  PREFIXO_ERRO = '#### ERRO: ';

type

  { TdtmAtualizaMod }

  TdtmAtualizaMod = class(TDataModule)
    ZConnection1: TZConnection;
    ZQuery1: TZQuery;
    ZReadOnlyQuery1: TZReadOnlyQuery;
  private
    { private declarations }
    function AbreSQLSelect(qry: TZReadOnlyQuery; cmd_sql: string;
      erro_com_sql:boolean=true): string;
  public
    { public declarations }
    TextoExcecao, MensagemConexao: string;
    function ConectaBanco(conex: TZConnection; ip_serv: string): boolean;
    function ExecutaSqlRetornaStringList(qry: TZReadOnlyQuery; comando_sql:string;var lista:TStringList;
      raw_data:boolean = false): integer;
    function ExecutaSqlRetornaString(qry: TZReadOnlyQuery; comando_sql:string;valor_default:
      string='';erro_com_sql:boolean = true): string;
    function GravaSQL(cmd_sql:string):string;
  end;

var
  dtmAtualizaMod: TdtmAtualizaMod;

implementation

uses strutils;

{$R *.lfm}

{ TdtmAtualizaMod }

function TdtmAtualizaMod.AbreSQLSelect(qry: TZReadOnlyQuery; cmd_sql: string;
  erro_com_sql: boolean): string;
begin
  with qry do
  begin
    Close;
    SQL.Clear;
    SQL.Add(cmd_sql);
    try
      Open;
    except
      on E:Exception do
      begin
        Result := IfThen(erro_com_sql,cmd_sql) + sLineBreak + E.Message;
        Exit;
      end;
    end;
    First;
  end;
  Result := 'OK';
end;

function TdtmAtualizaMod.ConectaBanco(conex: TZConnection; ip_serv: string): boolean;
begin
  with conex do
  begin
    Properties.Clear;
    if Connected and (HostName = ip_serv) then
    begin
      MensagemConexao := 'Active connection to ' + ip_serv;
      Result := True;
      Exit;
    end;
    Connected := False;
    HostName := ip_serv;
    //Password := DeCriptografa(md5_serial);
  end;
  MensagemConexao := '';
  try
    conex.Connected := True;
    Result := True;
  except
    on E: Exception do
    begin
      MensagemConexao := E.Message;
      Result := False;
      Exit;
    end;
  end;
  MensagemConexao := Format('Connected to "%s"',[ip_serv]);
end;

function TdtmAtualizaMod.ExecutaSqlRetornaStringList(qry: TZReadOnlyQuery;
  comando_sql: string; var lista: TStringList; raw_data: boolean): integer;
var i: integer;
  linha: string;
  resp: string;
begin
  Result := -1;
  lista.Clear;
  resp := AbreSQLSelect(qry,comando_sql);
  if resp <> 'OK' then
  begin
    TextoExcecao := PREFIXO_ERRO+resp;
    Exit;
  end;
  with qry do
  begin
    First;
    //--- laco principal
    while not EOF do
    begin
      linha := '';
      for i:=0 to FieldCount-1 do
        if raw_data then
          linha := linha + Fields.Fields[i].AsString
        else
          linha := linha + StringReplace(Fields.Fields[i].AsString,'|',' ',[rfReplaceAll]) + '|';
      lista.Add(linha);
      Next;
    end;
    Result := FieldCount;
    Close;
  end;
end;

function TdtmAtualizaMod.ExecutaSqlRetornaString(qry: TZReadOnlyQuery;
  comando_sql: string; valor_default: string; erro_com_sql: boolean): string;
var mValor: variant;
  resp: string;
begin
  Result := valor_default;
  resp := AbreSQLSelect(qry, comando_sql, erro_com_sql);
  if resp <> 'OK' then
  begin
    Result := resp;
    Exit;
  end;
  with qry do
  begin
    if not EOF then
      mValor := Fields.Fields[0].AsString;
    if not VarIsNull(mValor) then
      Result := mValor;
    Close;
  end;
end;

function TdtmAtualizaMod.GravaSQL(cmd_sql: string): string;
begin
  with ZQuery1 do
  begin
    try
      Close;
      SQL.Clear;
      SQL.Add(cmd_sql);
      ExecSQL;
      Close;
    except
      on E:Exception do
      begin
        Result := cmd_sql + sLineBreak + '#### ' + E.Message;
        Exit;
      end;
    end;
  end;
  Result := 'OK';
end;

end.

