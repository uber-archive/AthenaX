<#--
// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
-->

private void FunctionJarDef(List<SqlNode> list) :
{
    SqlParserPos pos;
    SqlNode uri;
}
{
    ( <JAR> | <FILE> | <ARCHIVE> )
    {
        pos = getPos();
        list.add(StringLiteral());
    }
}

SqlNodeList FunctionJarDefList() :
{
    SqlParserPos pos;
    List<SqlNode> list = Lists.newArrayList();
}
{
    <USING> { pos = getPos(); }
    { pos = getPos(); }
    FunctionJarDef(list)
    ( <COMMA> FunctionJarDef(list) )*
    {
        return new SqlNodeList(list, pos.plus(getPos()));
    }
}

/**
 * CREATE FUNCTION [db_name.]function_name AS class_name
 *   [USING JAR|FILE|ARCHIVE 'file_uri' [, JAR|FILE|ARCHIVE 'file_uri']
 */
SqlCreateFunction SqlCreateFunction() :
{
    SqlParserPos pos;
    SqlIdentifier dbName = null;
    SqlIdentifier funcName;
    SqlNode className;
    SqlNodeList jarList = null;
}
{
    <CREATE> { pos = getPos(); }
    <FUNCTION>
    [
        dbName = SimpleIdentifier()
        <DOT>
    ]

    funcName = SimpleIdentifier()
    <AS>
    className = StringLiteral()
    [
        jarList = FunctionJarDefList()
    ]
    {
        return new SqlCreateFunction(pos, dbName, funcName, className, jarList);
    }
}


private void SqlStmtList(SqlNodeList list) :
{
}
{
    {
        list.add(SqlStmt());
    }
}

SqlNodeList SqlStmtsEof() :
{
    SqlParserPos pos;
    SqlNodeList stmts;
}
{
    {
        pos = getPos();
        stmts = new SqlNodeList(pos);
        stmts.add(SqlStmt());
    }
    ( LOOKAHEAD(2, <SEMICOLON> SqlStmt()) <SEMICOLON> SqlStmtList(stmts) )*
    [ <SEMICOLON> ] <EOF>
    {
        return stmts;
    }
}

