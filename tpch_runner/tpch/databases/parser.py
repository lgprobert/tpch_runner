import sqlparse
from sqlparse.sql import Comparison, Identifier, IdentifierList, Parenthesis, Where
from sqlparse.tokens import Comment, Keyword, Newline, Punctuation, Whitespace


def update_identifier(identifier, schema: str = "moxe"):
    """Prepend 'schema.' to table names in an Identifier."""
    if identifier.get_real_name() and "." not in identifier.get_real_name():
        identifier.value = f"{schema}.{identifier.get_real_name()}"
        if identifier.has_alias():
            identifier.value = f"{identifier.value} {identifier.get_alias()}"


def process_tokens(tokens, schema: str = "moxe", cte_names=None):
    """Recursively process SQL tokens and prepend schema name where necessary."""
    if cte_names is None:
        cte_names = set()

    tokens = [
        token for token in tokens if token.ttype not in [Whitespace, Newline, Comment]
    ]

    i = 0
    while i < len(tokens):
        token = tokens[i]
        # breakpoint()

        if token.value.upper() == "WITH":
            # breakpoint()
            cte_names.update(extract_cte_names(tokens, i))
            if i + 1 < len(tokens):
                cte_tokens = tokens[i + 1]
                process_tokens(cte_tokens.tokens, schema, cte_names)

        elif isinstance(token, Parenthesis):
            process_tokens(token.tokens, schema, cte_names)

        elif isinstance(token, Comparison):
            process_tokens(token.tokens, schema, cte_names)

        elif token.value.upper() in {"FROM", "JOIN"}:
            handle_tables_in_from(tokens, i, schema, cte_names)

        elif token.value.upper() in {"CREATE", "DROP", "INSERT"}:
            if i + 1 < len(tokens) and tokens[i + 1].value.upper() in [
                "TABLE",
                "VIEW",
                "INTO",
            ]:
                next_token = tokens[i + 2] if i + 2 < len(tokens) else None
                if isinstance(next_token, Identifier):
                    update_identifier(next_token, schema)

        elif token.value.upper() in {"GROUP BY", "ORDER BY"}:
            pass

        elif isinstance(token, Where):
            _tokens = [t for t in token.tokens if t.ttype not in [Punctuation]]
            process_tokens(_tokens, schema, cte_names)

        elif token.ttype == Keyword.DML and token.value.upper() == "SELECT":

            j = i + 1
            while j < len(tokens):
                sub_token = tokens[j]
                if isinstance(sub_token, Parenthesis):
                    process_tokens(sub_token.tokens, schema, cte_names)
                    break
                if sub_token.value.upper() in {"FROM", "JOIN"}:
                    break
                j += 1

        i += 1


def extract_cte_names(tokens, start_idx):
    """Extract CTE names from a WITH clause."""
    cte_names = set()
    for token in tokens[start_idx:]:
        if isinstance(token, Identifier):
            cte_names.add(token.get_real_name())
        if token.ttype == Keyword.DML and token.value.upper() in {
            "SELECT",
            "INSERT",
            "UPDATE",
        }:
            break  # Stop when the actual query begins
    return cte_names


def handle_tables_in_from(tokens, i, schema, cte_names=None):
    """Handle tables in the FROM and JOIN clauses."""
    # breakpoint()
    next_token = tokens[i + 1] if i + 1 < len(tokens) else None
    # breakpoint()
    if isinstance(next_token, Identifier):
        if next_token.get_real_name() not in cte_names:
            update_identifier(next_token, schema)

    elif isinstance(next_token, IdentifierList):
        # Handle multiple tables
        updated_identifiers = []
        for identifier in next_token.get_identifiers():
            if identifier.get_real_name() not in cte_names:
                update_identifier(identifier, schema)
            updated_identifiers.append(identifier.value)
        next_token.value = ", ".join(updated_identifiers)

    elif isinstance(next_token, Parenthesis):
        process_tokens(next_token.tokens, schema, cte_names)


def join_tokens(tokens):
    collected_values = []
    for token in tokens:
        if isinstance(token, Parenthesis):
            collected_values.extend(join_tokens(token.tokens))
        elif "tokens" in dir(token) and not isinstance(token, Identifier):
            collected_values.extend(join_tokens(token.tokens))
        elif (
            "tokens" in dir(token)
            and isinstance(token, Identifier)
            and not token.tokens[0].ttype
        ):
            collected_values.extend(join_tokens(token.tokens))
        else:
            collected_values.append(token.value)
    return collected_values


def add_schema_to_table_names(sql, schema: str = "moxe"):

    parsed = sqlparse.parse(sql)
    modified_statements = []
    raw_tokens = []

    for statement in parsed:
        process_tokens(statement.tokens, schema=schema)

        # breakpoint()
        modified_statement = "".join(join_tokens(statement.tokens))
        modified_statements.append(modified_statement)
        raw_tokens.append(statement.tokens)

    # return " ".join(modified_statements), raw_tokens
    return " ".join(modified_statements)
