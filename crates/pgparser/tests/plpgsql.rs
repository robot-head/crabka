use assert2::assert;
use crabka_pgparser::{
    ast::{
        PlPgSqlDeclaration, PlPgSqlLoop, PlPgSqlRaiseLevel, PlPgSqlStatement,
        PlPgSqlVariableConflict, Statement,
    },
    parse_plpgsql,
};

#[test]
fn parses_variable_conflict_directive() {
    for (setting, expected) in [
        ("error", PlPgSqlVariableConflict::Error),
        ("use_variable", PlPgSqlVariableConflict::UseVariable),
        ("use_column", PlPgSqlVariableConflict::UseColumn),
    ] {
        let block = parse_plpgsql(&format!("#variable_conflict {setting}\nbegin null; end"))
            .expect("directive");
        assert!(block.variable_conflict == expected);
    }

    let error = parse_plpgsql("#variable_conflict guessing\nbegin null; end")
        .expect_err("invalid directive setting");
    assert!(
        error
            .message
            .contains("unrecognized #variable_conflict setting")
    );
}

#[test]
fn parses_declarations_assignments_sql_and_exceptions() {
    let block = parse_plpgsql(
        r"
        declare
          total constant int not null := 0;
          answer int default 42;
        begin
          total := total + answer;
          select answer into strict total;
          perform abs(total);
        exception
          when division_by_zero or numeric_value_out_of_range then
            raise warning 'bad value: %', total using hint = 'retry';
          when others then
            null;
        end
        ",
    )
    .expect("PL/pgSQL block");

    assert!(block.declarations.len() == 2);
    let PlPgSqlDeclaration::Variable {
        constant,
        not_null,
        default,
        ..
    } = &block.declarations[0]
    else {
        panic!("expected variable declaration");
    };
    assert!(*constant && *not_null && default.is_some());
    assert!(matches!(
        block.statements[0],
        PlPgSqlStatement::Assign { .. }
    ));
    let PlPgSqlStatement::Sql {
        into: Some(into), ..
    } = &block.statements[1]
    else {
        panic!("expected SELECT INTO");
    };
    assert!(into.strict);
    assert!(block.exceptions.len() == 2);
    let PlPgSqlStatement::Raise(raise) = &block.exceptions[0].statements[0] else {
        panic!("expected RAISE");
    };
    assert!(raise.level == PlPgSqlRaiseLevel::Warning);
}

#[test]
fn rejects_duplicate_names_in_one_declaration_scope() {
    for declarations in [
        "item int; item text;",
        "item alias for $1; item alias for $2;",
        "item cursor for select 1; item cursor for select 2;",
        "item int; item alias for $1;",
        "item alias for $1; item cursor for select 1;",
        "item cursor for select 1; item int;",
    ] {
        let error = parse_plpgsql(&format!("declare {declarations} begin null; end"))
            .expect_err("duplicate declaration");
        assert!(error.message.contains("duplicate declaration \"item\""));
    }
}

#[test]
fn nested_blocks_may_reuse_declaration_names() {
    parse_plpgsql("declare item int; begin declare item text; begin null; end; end")
        .expect("nested declaration scope");
}

#[test]
fn parses_percent_type_declarations() {
    let block = parse_plpgsql("declare copy value%type; begin null; end").expect("%TYPE");
    let PlPgSqlDeclaration::Variable { ty, .. } = &block.declarations[0] else {
        panic!("expected variable declaration");
    };
    assert!(ty.resolved.is_none());
    assert!(ty.name == "value%type");
}

#[test]
fn parses_if_case_and_all_loop_sources() {
    let block = parse_plpgsql(
        r"
        begin
          if n = 1 then
            null;
          elsif n = 2 then
            null;
          else
            null;
          end if;
          case n
            when 1, 2 then null;
            else null;
          end case;
          loop exit; end loop;
          while n < 3 loop n := n + 1; end loop;
          for i in reverse 10..1 by 2 loop continue when i = 4; end loop;
          for r in select * from things loop exit; end loop;
          for r in execute 'select * from things' using n loop exit; end loop;
          foreach item, value slice 1 in array values_array loop exit; end loop;
        end
        ",
    )
    .expect("control structures");

    assert!(matches!(block.statements[0], PlPgSqlStatement::If { .. }));
    assert!(matches!(block.statements[1], PlPgSqlStatement::Case { .. }));
    let PlPgSqlStatement::Loop { kind, .. } = &block.statements[4] else {
        panic!("expected integer loop");
    };
    assert!(matches!(
        kind.as_ref(),
        PlPgSqlLoop::Integer { reverse: true, .. }
    ));
    let PlPgSqlStatement::Loop { kind, .. } = &block.statements[5] else {
        panic!("expected query loop");
    };
    assert!(matches!(kind.as_ref(), PlPgSqlLoop::Query { .. }));
    let PlPgSqlStatement::Loop { kind, .. } = &block.statements[6] else {
        panic!("expected dynamic loop");
    };
    assert!(matches!(kind.as_ref(), PlPgSqlLoop::Dynamic { .. }));
    let PlPgSqlStatement::Loop { kind, .. } = &block.statements[7] else {
        panic!("expected foreach loop");
    };
    let PlPgSqlLoop::Foreach { targets, slice, .. } = kind.as_ref() else {
        panic!("expected foreach source");
    };
    assert!(targets.len() == 2 && *slice == Some(1));
}

#[test]
fn validates_control_scope_and_end_labels() {
    parse_plpgsql(
        r"
        <<outer>> begin
          <<again>> while true loop
            continue again;
            exit outer when false;
          end loop again;
        end outer
        ",
    )
    .expect("valid labels");

    let error = parse_plpgsql("begin continue; end").expect_err("continue outside loop");
    assert!(error.message.contains("outside a loop"));
    let error = parse_plpgsql("<<a>> begin null; end b").expect_err("mismatched label");
    assert!(error.message.contains("differs"));
    let error = parse_plpgsql("<<b>> begin continue b; end b")
        .expect_err("block label is not a continue target");
    assert!(error.message.contains("cannot be used in CONTINUE"));
}

#[test]
fn validates_labeled_blocks_with_declarations() {
    let block = parse_plpgsql(
        r"
        begin
          <<with_vars>>
          declare
            answer int := 42;
          begin
            null;
          end with_vars;
        end
        ",
    )
    .expect("labeled block with declarations");

    let PlPgSqlStatement::Block(inner) = &block.statements[0] else {
        panic!("expected nested block");
    };
    assert!(inner.label.as_deref() == Some("with_vars"));
    assert!(inner.declarations.len() == 1);
    assert!(inner.end_label.as_deref() == Some("with_vars"));

    let error = parse_plpgsql("begin <<invalid>> null; end")
        .expect_err("label before a non-block statement");
    assert!(
        error
            .message
            .contains("a label must precede a block or loop")
    );

    let error = parse_plpgsql("begin <<expected>> declare answer int; begin null; end actual; end")
        .expect_err("mismatched declared block label");
    assert!(
        error
            .message
            .contains("end label \"actual\" differs from block's label \"expected\"")
    );
}

#[test]
fn parses_return_raise_and_dynamic_execution() {
    let block = parse_plpgsql(
        r"
        begin
          return next value;
          return query select * from things;
          return query execute 'select * from things where id = $1' using wanted;
          execute command into strict result using argument;
          execute command using first_argument, second_argument into strict result;
          raise sqlstate '22012' using message = 'division by zero';
          return result;
        end
        ",
    )
    .expect("returns and dynamic SQL");

    assert!(matches!(
        block.statements[0],
        PlPgSqlStatement::ReturnNext(_)
    ));
    assert!(matches!(
        &block.statements[1],
        PlPgSqlStatement::ReturnQuery {
            source,
            line: 4,
            ..
        } if source == "select * from things"
    ));
    assert!(matches!(
        block.statements[2],
        PlPgSqlStatement::ReturnQueryExecute { .. }
    ));
    assert!(matches!(
        block.statements[3],
        PlPgSqlStatement::Execute { .. }
    ));
    assert!(matches!(
        &block.statements[4],
        PlPgSqlStatement::Execute { into: Some(into), using, .. }
        if into.strict && into.targets.len() == 1 && using.len() == 2
    ));
}

#[test]
fn parses_cursor_and_diagnostics_statements() {
    let block = parse_plpgsql(
        r"
        declare
          rows_cur scroll cursor(limit_rows int) for select * from things limit limit_rows;
          dynamic_cur refcursor;
        begin
          open rows_cur(10);
          open dynamic_cur no scroll for execute query_text using argument;
          fetch next from rows_cur into row_value;
          move backward 2 from rows_cur;
          close rows_cur;
          get diagnostics affected = row_count;
          get stacked diagnostics state = returned_sqlstate, message = message_text;
        end
        ",
    )
    .expect("cursor statements");

    assert!(matches!(
        block.declarations[0],
        PlPgSqlDeclaration::Cursor { .. }
    ));
    assert!(matches!(block.statements[0], PlPgSqlStatement::Open { .. }));
    assert!(matches!(
        block.statements[2],
        PlPgSqlStatement::Fetch { .. }
    ));
    assert!(matches!(
        block.statements[3],
        PlPgSqlStatement::Fetch {
            move_only: true,
            ..
        }
    ));
    assert!(matches!(
        block.statements[5],
        PlPgSqlStatement::GetDiagnostics { stacked: false, .. }
    ));
    assert!(matches!(
        block.statements[6],
        PlPgSqlStatement::GetDiagnostics { stacked: true, .. }
    ));
}

#[test]
fn parses_transaction_control() {
    let block = parse_plpgsql("begin commit and chain; rollback and no chain; null; end")
        .expect("transaction control");
    assert!(matches!(
        block.statements[0],
        PlPgSqlStatement::Transaction {
            commit: true,
            chain: true
        }
    ));
    assert!(matches!(
        block.statements[1],
        PlPgSqlStatement::Transaction {
            commit: false,
            chain: false
        }
    ));
}

#[test]
fn static_sql_keeps_one_native_statement() {
    let block = parse_plpgsql("begin insert into things values (1); end").expect("static INSERT");
    let PlPgSqlStatement::Sql {
        statement, into, ..
    } = &block.statements[0]
    else {
        panic!("expected static SQL");
    };
    assert!(matches!(statement.as_ref(), Statement::Insert { .. }));
    assert!(into.is_none());
}

#[test]
fn parses_static_dml_returning_into_without_confusing_sql_into() {
    for sql in [
        "insert into things values (1) returning id into strict result",
        "update things set id = id + 1 returning id into result",
        "delete from things returning id into result",
        "merge into things using source on things.id = source.id when matched then update set id = source.id returning things.id into result",
        "with source as (select 1 as id) insert into things select id from source returning id into result",
    ] {
        let block = parse_plpgsql(&format!("begin {sql}; end")).expect("static DML INTO");
        let PlPgSqlStatement::Sql {
            statement,
            into: Some(into),
            ..
        } = &block.statements[0]
        else {
            panic!("expected static DML INTO for `{sql}`");
        };
        assert!(into.targets[0].path == ["result"]);
        assert!(
            matches!(
                statement.as_ref(),
                Statement::Insert {
                    returning: Some(_),
                    ..
                }
            ) || matches!(
                statement.as_ref(),
                Statement::Update {
                    returning: Some(_),
                    ..
                }
            ) || matches!(
                statement.as_ref(),
                Statement::Delete {
                    returning: Some(_),
                    ..
                }
            ) || matches!(
                statement.as_ref(),
                Statement::Merge {
                    returning: Some(_),
                    ..
                }
            )
        );
    }
}

#[test]
fn parses_select_into_after_cte() {
    let block = parse_plpgsql(
        r"begin
          with progress_data as (
            select pid, relid, command, type, bytes_processed, bytes_total,
                   tuples_processed, tuples_excluded
            from pg_stat_progress_copy where pid = pg_backend_pid()
          )
          select into report (to_jsonb(r)) as value from progress_data r;
        end",
    )
    .expect("CTE SELECT INTO");
    let PlPgSqlStatement::Sql {
        statement,
        into: Some(into),
        ..
    } = &block.statements[0]
    else {
        panic!("expected CTE SELECT INTO");
    };
    assert!(matches!(statement.as_ref(), Statement::Query(_)));
    assert!(into.targets[0].path == ["report"]);
}

#[test]
fn ignores_nested_dml_returning_when_extracting_static_into() {
    let block = parse_plpgsql(
        "begin with moved as (delete from things returning id) insert into archive select id from moved; end",
    )
    .expect("data-modifying CTE");
    let PlPgSqlStatement::Sql {
        statement, into, ..
    } = &block.statements[0]
    else {
        panic!("expected static SQL");
    };
    assert!(matches!(statement.as_ref(), Statement::Insert { .. }));
    assert!(into.is_none());
}

#[test]
fn parses_assert_with_optional_message() {
    let block = parse_plpgsql(
        "begin assert total > 0; assert pair = row(1, 2), format('bad: %s', pair); end",
    )
    .expect("ASSERT statements");

    assert!(matches!(
        block.statements.as_slice(),
        [
            PlPgSqlStatement::Assert { message: None, .. },
            PlPgSqlStatement::Assert {
                message: Some(_),
                ..
            }
        ]
    ));
    assert!(parse_plpgsql("begin assert; end").is_err());
    assert!(parse_plpgsql("begin assert true,; end").is_err());
}

const POSTGRES_18_SIMPLE_VALID: &[(&str, &str)] = &[
    (
        "inline_function_change",
        r"
declare
  sum int := 0;
begin
  for n in 1..10 loop
    sum := sum + simplesql(n);
    if n = 5 then
      create or replace function simplesql(int) returns int language sql
      as 'select $1 + 100';
    end if;
  end loop;
  return sum;
end",
    ),
    ("schema_qualified_target", r"begin return $1; end"),
    ("public_target", r"begin return $1 + 100; end"),
    (
        "search_path_change",
        r"
declare
  sum int := 0;
begin
  for n in 1..10 loop
    sum := sum + simpletarget(n);
    if n = 5 then
      set local search_path = 'simple1';
    end if;
  end loop;
  return sum;
end",
    ),
    (
        "simple_select",
        r"
declare x int;
begin
  select simplesql() into x;
  return x;
end",
    ),
    (
        "scalar_to_set_function_change",
        r"
declare x int;
begin
  x := simplesql();
  return x;
end",
    ),
    (
        "scrollable_cursor",
        r"
declare
 p_CurData refcursor;
 val int;
begin
 open p_CurData scroll for select 42;
 fetch p_CurData into val;
 raise notice 'val = %', val;
end; ",
    ),
];

const POSTGRES_18_CONTROL_VALID: &[(&str, &str)] = &[
    (
        "integer_for_cases",
        r"
begin
  -- basic case
  for i in 1..3 loop
    raise notice '1..3: i = %', i;
  end loop;
  -- with BY, end matches exactly
  for i in 1..10 by 3 loop
    raise notice '1..10 by 3: i = %', i;
  end loop;
  -- with BY, end does not match
  for i in 1..11 by 3 loop
    raise notice '1..11 by 3: i = %', i;
  end loop;
  -- zero iterations
  for i in 1..0 by 3 loop
    raise notice '1..0 by 3: i = %', i;
  end loop;
  -- REVERSE
  for i in reverse 10..0 by 3 loop
    raise notice 'reverse 10..0 by 3: i = %', i;
  end loop;
  -- potential overflow
  for i in 2147483620..2147483647 by 10 loop
    raise notice '2147483620..2147483647 by 10: i = %', i;
  end loop;
  -- potential overflow, reverse direction
  for i in reverse -2147483620..-2147483647 by 10 loop
    raise notice 'reverse -2147483620..-2147483647 by 10: i = %', i;
  end loop;
end",
    ),
    (
        "zero_by_step",
        r"
begin
  for i in 1..3 by 0 loop
    raise notice '1..3 by 0: i = %', i;
  end loop;
end",
    ),
    (
        "negative_by_step",
        r"
begin
  for i in 1..3 by -1 loop
    raise notice '1..3 by -1: i = %', i;
  end loop;
end",
    ),
    (
        "reverse_negative_by_step",
        r"
begin
  for i in reverse 1..3 by -1 loop
    raise notice 'reverse 1..3 by -1: i = %', i;
  end loop;
end",
    ),
    (
        "continue_test",
        r"
declare _i integer = 0; _r record;
begin
  raise notice '---1---';
  loop
    _i := _i + 1;
    raise notice '%', _i;
    continue when _i < 10;
    exit;
  end loop;

  raise notice '---2---';
  <<lbl>>
  loop
    _i := _i - 1;
    loop
      raise notice '%', _i;
      continue lbl when _i > 0;
      exit lbl;
    end loop;
  end loop;

  raise notice '---3---';
  <<the_loop>>
  while _i < 10 loop
    _i := _i + 1;
    continue the_loop when _i % 2 = 0;
    raise notice '%', _i;
  end loop;

  raise notice '---4---';
  for _i in 1..10 loop
    begin
      -- applies to outer loop, not the nested begin block
      continue when _i < 5;
      raise notice '%', _i;
    end;
  end loop;

  raise notice '---5---';
  for _r in select * from conttesttbl loop
    continue when _r.v <= 20;
    raise notice '%', _r.v;
  end loop;

  raise notice '---6---';
  for _r in execute 'select * from conttesttbl' loop
    continue when _r.v <= 20;
    raise notice '%', _r.v;
  end loop;

  raise notice '---7---';
  <<looplabel>>
  for _i in 1..3 loop
    continue looplabel when _i = 2;
    raise notice '%', _i;
  end loop;

  raise notice '---8---';
  _i := 1;
  while _i <= 3 loop
    raise notice '%', _i;
    _i := _i + 1;
    continue when _i = 3;
  end loop;

  raise notice '---9---';
  for _r in select * from conttesttbl order by v limit 1 loop
    raise notice '%', _r.v;
    continue;
  end loop;

  raise notice '---10---';
  for _r in execute 'select * from conttesttbl order by v limit 1' loop
    raise notice '%', _r.v;
    continue;
  end loop;

  raise notice '---11---';
  <<outerlooplabel>>
  for _i in 1..2 loop
    raise notice 'outer %', _i;
    <<innerlooplabel>>
    for _j in 1..3 loop
      continue outerlooplabel when _j = 2;
      raise notice 'inner %', _j;
    end loop;
  end loop;
end; ",
    ),
    (
        "labeled_block_exit",
        r"
begin
    <<begin_block1>>
    begin
        loop
            exit begin_block1;
            raise exception 'should not get here';
        end loop;
    end;
end;
",
    ),
    (
        "verbose_end_labels",
        r"
<<blbl>>
begin
  <<flbl1>>
  for i in 1 .. 10 loop
    raise notice 'i = %', i;
    exit flbl1;
  end loop flbl1;
  <<flbl2>>
  for j in 1 .. 10 loop
    raise notice 'j = %', j;
    exit flbl2;
  end loop;
end blbl;
",
    ),
    (
        "unlabeled_exit",
        r"
begin
for i in 1..10 loop
  <<innerblock>>
  begin
    begin  -- unlabeled block
      exit;
      raise notice 'should not get here';
    end;
    raise notice 'should not get here, either';
  end;
  raise notice 'nor here';
end loop;
raise notice 'should get here';
end",
    ),
    (
        "nested_labeled_block_exit",
        r"
<<outerblock>>
begin
  <<innerblock>>
  begin
    <<moreinnerblock>>
    begin
      begin  -- unlabeled block
        exit innerblock;
        raise notice 'should not get here';
      end;
      raise notice 'should not get here, either';
    end;
    raise notice 'nor here';
  end;
  raise notice 'should get here';
end",
    ),
    (
        "outermost_block_exit",
        r"
<<outerblock>>
begin
  <<innerblock>>
  begin
    exit outerblock;
    raise notice 'should not get here';
  end;
  raise notice 'should not get here, either';
end",
    ),
    (
        "while_exit",
        r"
begin
  <<outermostwhile>>
  while 1 > 0 loop
    <<outerwhile>>
    while 1 > 0 loop
      <<innerwhile>>
      while 1 > 0 loop
        exit;
        raise notice 'should not get here';
      end loop;
      raise notice 'should get here';
      exit outermostwhile;
      raise notice 'should not get here, either';
    end loop;
    raise notice 'nor here';
  end loop;
  raise notice 'should get here, too';
end",
    ),
    (
        "labeled_while_exit",
        r"
begin
  <<outerwhile>>
  while 1 > 0 loop
    while 1 > 0 loop
      exit outerwhile;
      raise notice 'should not get here';
    end loop;
    raise notice 'should not get here, either';
  end loop;
  raise notice 'should get here';
end",
    ),
    (
        "outer_while_continue",
        r"
declare i int := 0;
begin
  <<outermostwhile>>
  while i < 2 loop
    raise notice 'outermostwhile, i = %', i;
    i := i + 1;
    <<outerwhile>>
    while 1 > 0 loop
      <<innerwhile>>
      while 1 > 0 loop
        continue outermostwhile;
        raise notice 'should not get here';
      end loop;
      raise notice 'should not get here, either';
    end loop;
    raise notice 'nor here';
  end loop;
  raise notice 'out of outermostwhile, i = %', i;
end",
    ),
    (
        "return_from_while",
        r"
declare i int := 0;
begin
  while i < 10 loop
    if i > 2 then
      return i;
    end if;
    i := i + 1;
  end loop;
  return null;
end",
    ),
    (
        "scalar_and_record_for_targets",
        r"
<<lbl>>declare a integer; b varchar; c varchar; r record;
begin
  -- fori
  for i in 1 .. 3 loop
    raise notice '%', i;
  end loop;
  -- fore with record var
  for r in select gs as aa, 'BB' as bb, 'CC' as cc from generate_series(1,4) gs loop
    raise notice '% % %', r.aa, r.bb, r.cc;
  end loop;
  -- fore with single scalar
  for a in select gs from generate_series(1,4) gs loop
    raise notice '%', a;
  end loop;
  -- fore with multiple scalars
  for a,b,c in select gs, 'BB','CC' from generate_series(1,4) gs loop
    raise notice '% % %', a, b, c;
  end loop;
  -- using qualified names in fors, fore is enabled, disabled only for fori
  for lbl.a, lbl.b, lbl.c in execute $$select gs, 'bb','cc' from generate_series(1,4) gs$$ loop
    raise notice '% % %', a, b, c;
  end loop;
end;
",
    ),
    (
        "simple_case",
        r"
declare a int = 10;
        b int = 1;
begin
  case $1
    when 1 then
      return 'one';
    when 2 then
      return 'two';
    when 3,4,3+5 then
      return 'three, four or eight';
    when a then
      return 'ten';
    when a+b, a+b+1 then
      return 'eleven, twelve';
  end case;
end;
",
    ),
    (
        "case_not_found_handler",
        r"
begin
  raise notice '%', case_test(6);
exception
  when case_not_found then
    raise notice 'caught case_not_found % %', SQLSTATE, SQLERRM;
end
",
    ),
    (
        "searched_case",
        r"
declare a int = 10;
begin
  case
    when $1 = 1 then
      return 'one';
    when $1 = a + 2 then
      return 'twelve';
    else
      return 'other';
  end case;
end;
",
    ),
    (
        "case_line_comment",
        r"
begin
  case $1
    when 1 -- comment before THEN
      then return 'one';
    else
      return 'other';
  end case;
end;
",
    ),
];

const POSTGRES_18_TRAP_VALID: &[(&str, &str)] = &[
    (
        "zero_divide",
        r"
declare x int;
	sx smallint;
begin
	begin	-- start a subtransaction
		raise notice 'should see this';
		x := 100 / $1;
		raise notice 'should see this only if % <> 0', $1;
		sx := $1;
		raise notice 'should see this only if % fits in smallint', $1;
		if $1 < 0 then
			raise exception '% is less than zero', $1;
		end if;
	exception
		when division_by_zero then
			raise notice 'caught division_by_zero';
			x := -1;
		when NUMERIC_VALUE_OUT_OF_RANGE then
			raise notice 'caught numeric_value_out_of_range';
			x := -2;
	end;
	return x;
end",
    ),
    (
        "matching_categories",
        r"
declare x int;
	sx smallint;
	y int;
begin
	begin	-- start a subtransaction
		x := 100 / $1;
		sx := $1;
		select into y data from match_source where id =
			(select id from match_source b where ten = $1);
	exception
		when data_exception then  -- category match
			raise notice 'caught data_exception';
			x := -1;
		when NUMERIC_VALUE_OUT_OF_RANGE OR CARDINALITY_VIOLATION then
			raise notice 'caught numeric_value_out_of_range or cardinality_violation';
			x := -2;
	end;
	return x;
end",
    ),
    (
        "subtransaction_rollback",
        r"
declare x int;
begin
  x := 1;
  insert into foo values(x);
  begin
    x := x + 1;
    insert into foo values(x);
    raise exception 'inner';
  exception
    when others then
      x := x * 10;
  end;
  insert into foo values(x);
  return x;
end",
    ),
    (
        "timeout",
        r"
begin
  begin
    perform pg_sleep(10);
  exception
    when others then
      raise notice 'caught others: %', sqlerrm;
    when query_canceled then
      raise notice 'nyeah nyeah, can''t stop me';
  end;
  -- Abort transaction to abandon the statement_timeout setting.  Otherwise,
  -- the next top-level statement would be vulnerable to the timeout.
  raise exception 'end of function';
end",
    ),
    (
        "variable_storage",
        r"
declare x text;
begin
  x := '1234';
  begin
    x := x || '5678';
    -- force error inside subtransaction SPI context
    perform trap_zero_divide(-100);
  exception
    when others then
      x := x || '9012';
  end;
  return x;
end",
    ),
    (
        "foreign_key",
        r"
begin
	begin	-- start a subtransaction
		insert into leaf values($1);
	exception
		when foreign_key_violation then
			raise notice 'caught foreign_key_violation';
			return 0;
	end;
	return 1;
end",
    ),
    (
        "foreign_key_deferred",
        r"
begin
	begin	-- start a subtransaction
		set constraints all immediate;
	exception
		when foreign_key_violation then
			raise notice 'caught foreign_key_violation';
			return 0;
	end;
	return 1;
end",
    ),
];

const POSTGRES_18_CONTROL_INVALID: &[(&str, &str)] = &[
    (
        "continue_outside_loop",
        r"
begin
    begin
        continue;
    end;
end;
",
    ),
    (
        "exit_outside_loop",
        r"
begin
    begin
        exit;
    end;
end;
",
    ),
    (
        "continue_unknown_label",
        r"
begin
    begin
        loop
            continue no_such_label;
        end loop;
    end;
end;
",
    ),
    (
        "exit_unknown_label",
        r"
begin
    begin
        loop
            exit no_such_label;
        end loop;
    end;
end;
",
    ),
    (
        "continue_to_block_label",
        r"
begin
    <<begin_block1>>
    begin
        loop
            continue begin_block1;
        end loop;
    end;
end;
",
    ),
    (
        "undefined_loop_end_label",
        r"
begin
  for _i in 1 .. 10 loop
    exit;
  end loop flbl1;
end;
",
    ),
    (
        "mismatched_loop_end_label",
        r"
<<outer_label>>
begin
  <<inner_label>>
  for _i in 1 .. 10 loop
    exit;
  end loop outer_label;
end;
",
    ),
    (
        "loop_end_label_without_start",
        r"
<<outer_label>>
begin
  for _i in 1 .. 10 loop
    exit;
  end loop outer_label;
end;
",
    ),
];

#[test]
fn parses_every_postgres_18_regression_body() {
    let suites = [
        ("plpgsql_simple.sql", 7, POSTGRES_18_SIMPLE_VALID),
        ("plpgsql_control.sql", 19, POSTGRES_18_CONTROL_VALID),
        ("plpgsql_trap.sql", 7, POSTGRES_18_TRAP_VALID),
    ];

    for (suite, expected_count, cases) in suites {
        assert!(
            cases.len() == expected_count,
            "{suite}: expected {expected_count} corpus bodies"
        );
        for (case, body) in cases {
            if let Err(error) = parse_plpgsql(body) {
                panic!("{suite}/{case}: {}", error.message);
            }
        }
    }
}

#[test]
fn rejects_every_intentional_postgres_18_control_error() {
    assert!(
        POSTGRES_18_CONTROL_INVALID.len() == 8,
        "plpgsql_control.sql: expected 8 intentional-invalid bodies"
    );
    for (case, body) in POSTGRES_18_CONTROL_INVALID {
        assert!(
            parse_plpgsql(body).is_err(),
            "plpgsql_control.sql/{case} should be rejected"
        );
    }
}

#[test]
fn rejects_multiple_integer_loop_targets() {
    let error = parse_plpgsql("begin for a, b in 1..3 loop null; end loop; end")
        .expect_err("integer FOR target must be scalar");
    assert!(error.message.contains("one scalar variable"));
}

#[test]
fn query_for_is_not_confused_by_a_later_integer_range() {
    let block = parse_plpgsql(
        r"
        begin
          for row_value in select * from things loop null; end loop;
          for index_value in 1..3 loop null; end loop;
        end
        ",
    )
    .expect("query FOR followed by integer FOR");

    let PlPgSqlStatement::Loop { kind, .. } = &block.statements[0] else {
        panic!("expected query loop");
    };
    assert!(matches!(kind.as_ref(), PlPgSqlLoop::Query { .. }));
}
