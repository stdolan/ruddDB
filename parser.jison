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

start_state : table_expr EOF { console.log($1); } ;
table_expr
    : SELECT L_PAREN table_expr COMMA FUNCTION R_PAREN
	{ $$ = "sigma_" + $5 + "(" + $3 + ")"; }
	| JOIN L_PAREN table_expr COMMA table_expr R_PAREN
	{ $$ = "cross(" + $3 + ", " + $5 + ")"; }
	| PROJECT L_PAREN table_expr COMMA schema R_PAREN
	{ $$ = "pi_" + $5 + "(" + $3 + ")"; }
	| UNION L_PAREN table_expr COMMA table_expr R_PAREN
	{ $$ = "union(" + $3 + ", " + $5 + ")"; }
	| IDENTIFIER
	;
schema
    : L_BRACK id_list R_BRACK
	{ $$ = "[" + $2 + "]"; }
	| L_BRACK R_BRACK
	{ $$ = "[]"; }
	;
id_list
    : IDENTIFIER
	{ $$ = $1; }
	| IDENTIFIER COMMA id_list
	{ $$ = $1 + ", " + $3; }
	;