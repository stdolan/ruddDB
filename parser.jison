%lex
%%

\s+                       // Do nothing
'SELECT'                  return 'SELECT'
'JOIN'                    return 'JOIN'
'PROJECT'                 return 'PROJECT'
'UNION'                   return 'UNION'
'('                       return 'L_PAREN'
')'                       return 'R_PAREN'
'['                       return 'L_BRACK'
']'                       return 'R_BRACK'
','                       return 'COMMA'
\`([^\`]*)\`              return 'FUNCTION'
[a-zA-Z_][a-zA-Z0-9_]*    return 'IDENTIFIER'
<<EOF>>                   return 'EOF'
.                         return 'INVALID'

/lex

%start start_state

%%

start_state : table_expr EOF { return $1; } ;
table_expr
    : SELECT L_PAREN table_expr COMMA FUNCTION R_PAREN
	{ $$ = "new nodes.SelectNode(" + $3 + ", '" + $5.substring(1, $5.length - 1) + "')"; }
	| JOIN L_PAREN table_expr COMMA table_expr R_PAREN
	{ $$ = "new nodes.JoinNode(" + $3 + "," + $5 + ")"; }
	| PROJECT L_PAREN table_expr COMMA schema R_PAREN
	{ $$ = "new nodes.ProjectNode(" + $3 + "," + $5 + ")"; }
	| UNION L_PAREN table_expr COMMA table_expr R_PAREN
	{ $$ = "new nodes.UnionNode(" + $3 + "," + $5 + ")"; }
	| IDENTIFIER
	{ $$ = "new nodes.TableNode(" + $1 + ")"; }
	;
schema
    : L_BRACK id_list R_BRACK
	{ $$ = "'[" + $2 + "]'"; }
	| L_BRACK R_BRACK
	{ $$ = "[]"; }
	;
id_list
    : IDENTIFIER
	{ $$ = $1; }
	| IDENTIFIER COMMA id_list
	{ $$ = $1 + ", " + $3; }
	;